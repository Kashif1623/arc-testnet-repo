import os
import time
import threading
import queue
import sqlite3
import requests
from flask import Flask, Response, jsonify

# ==========================================
# CONFIGURATION (TESTNET)
# ==========================================
PRIMARY_RPC_ENDPOINTS = [
    "https://arc-testnet.drpc.org",
    "https://rpc.testnet.arc.network",
]
FALLBACK_RPC_ENDPOINTS = [
    "https://rpc.testnet.arc.io",
]

TELEGRAM_BOT_TOKEN = "8957473691:AAG6BgDwvyejUpEscgs9qcnGT-ddgtvrIEA"
TELEGRAM_CHAT_ID = "8822300532"
EXPECTED_CHAIN_ID = 5042002  # Arc testnet (mainnet is 5042)
EXPLORER = "https://explorer.testnet.arc.io"
FAILURE_THRESHOLD = 3
COOLDOWN_SECONDS = 60
MAX_DRIFT_THRESHOLD = 8
SUPER_PATIENT_TIMEOUT = 6
GAS_ALERT_THRESHOLD_GWEI = 150.0
HIGH_LATENCY_THRESHOLD_MS = 500
POLL_SECONDS = 15

# Arc gas accounting = USDC wei (18 decimals). Display dollar, not ETH.
NATIVE_GAS = 21000
ERC20_GAS = 50000

DB_FILE = "arc_testnet_sla.db"
ACTIVE_RPC_POOL = list(PRIMARY_RPC_ENDPOINTS)
rpc_status = {
    url: {"failures": 0, "circuit_broken_until": 0, "alerted": False}
    for url in (PRIMARY_RPC_ENDPOINTS + FALLBACK_RPC_ENDPOINTS)
}
global_node_data = {}
watch_addresses = []  # /watch 0x... se bharti hai

app = Flask(__name__)
log_queue = queue.Queue(maxsize=200)

# ==========================================
# SQLITE
# ==========================================
def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS uptime_logs
               (timestamp TEXT, node_url TEXT, status TEXT, latency_ms INTEGER, block_height INTEGER)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS watchlist
               (address TEXT PRIMARY KEY, last_balance TEXT)"""
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

def log_to_db(url, status, latency=0, block=0):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO uptime_logs VALUES (?, ?, ?, ?, ?)",
            (timestamp, url, status, latency, block),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

def sla_percent(hours=24):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - hours * 3600))
        c.execute(
            "SELECT status, COUNT(*) FROM uptime_logs WHERE timestamp >= ? GROUP BY status",
            (cutoff,),
        )
        rows = dict(c.fetchall())
        conn.close()
        online = rows.get("ONLINE", 0)
        total = online + rows.get("OFFLINE", 0)
        if total == 0:
            return None
        return round(100.0 * online / total, 2)
    except Exception:
        return None

init_db()

def log_msg(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"{timestamp} | {message}"
    print(formatted)
    try:
        if log_queue.full():
            log_queue.get_nowait()
        log_queue.put_nowait(formatted)
    except Exception:
        pass

# ==========================================
# RPC + USDC GAS
# ==========================================
def rpc_call(url, method, params=None):
    payload = {"jsonrpc": "2.0", "method": method, "params": params or [], "id": 1}
    resp = requests.post(url, json=payload, timeout=SUPER_PATIENT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise ValueError(data["error"])
    return data["result"]

def fmt_usdc(value):
    if value is None:
        return "n/a"
    if value < 0.0001:
        return f"${value:.6f}"
    return f"${value:.4f}"

def fetch_gas(url):
    gas_wei = int(rpc_call(url, "eth_gasPrice"), 16)
    gwei = gas_wei / 1e9
    base_gwei = None
    try:
        hist = rpc_call(url, "eth_feeHistory", ["0x1", "latest", [50]])
        base_gwei = int(hist["baseFeePerGas"][-1], 16) / 1e9
    except Exception:
        pass
    return {
        "gwei": round(gwei, 2),
        "base_gwei": round(base_gwei, 2) if base_gwei is not None else None,
        "native_usdc": (NATIVE_GAS * gas_wei) / 1e18,
        "erc20_usdc": (ERC20_GAS * gas_wei) / 1e18,
    }

def fetch_tx_count(url):
    block = rpc_call(url, "eth_getBlockByNumber", ["latest", False])
    if not block:
        return 0
    return len(block.get("transactions") or [])

def fetch_balance(url, address):
    raw = int(rpc_call(url, "eth_getBalance", [address, "latest"]), 16)
    return raw / 1e18

def healthy_url():
    for url, data in global_node_data.items():
        if data.get("status") == "ONLINE":
            return url
    return PRIMARY_RPC_ENDPOINTS[0]

# ==========================================
# TELEGRAM
# ==========================================
def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_msg("[!] Telegram token/chat id blank — alert skipped")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        log_msg(f"[!] Telegram API Error: {e}")

def send_custom_message(chat_id, message):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "reply_markup": {
                "inline_keyboard": [[
                    {"text": "Status", "callback_data": "/status"},
                    {"text": "Gas", "callback_data": "/gas"},
                    {"text": "SLA", "callback_data": "/sla"},
                ]]
            },
        }
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        log_msg(f"[!] Telegram Custom Message Error: {e}")

def get_status_report():
    report = "⚡ *ARC Testnet Node Status*\n\n"
    blocks = []
    for url, data in global_node_data.items():
        status = data.get("status", "UNKNOWN")
        block = data.get("block", 0)
        latency = data.get("latency", 0)
        txs = data.get("txs", 0)
        gas = data.get("gas") or {}
        emoji = "🟢" if status == "ONLINE" else "🔴"
        if status == "ONLINE" and block:
            blocks.append(block)
        report += (
            f"{emoji} `{url}`\n"
            f"   • Status: *{status}*\n"
            f"   • Block: `{block}`\n"
            f"   • Latest txs: `{txs}`\n"
            f"   • Latency: `{latency}ms`\n"
            f"   • Gas: `{gas.get('gwei', 'n/a')} gwei`\n"
            f"   • Native send: `{fmt_usdc(gas.get('native_usdc'))}`\n"
            f"   • ERC-20 xfer: `{fmt_usdc(gas.get('erc20_usdc'))}`\n\n"
        )
    if blocks:
        drift = max(blocks) - min(blocks)
        report += f"📏 Block drift: `{drift}` (alert > {MAX_DRIFT_THRESHOLD})\n"
    sla = sla_percent(24)
    if sla is not None:
        report += f"📈 24h SLA: `{sla}%`\n"
    report += f"🔗 Explorer: {EXPLORER}\n"
    if not global_node_data:
        report += "Initializing nodes, wait a few seconds..."
    return report

def get_gas_report():
    url = healthy_url()
    try:
        gas = fetch_gas(url)
    except Exception as e:
        return f"🔴 Gas fetch failed on `{url}`\n`{e}`"
    return (
        "💵 *ARC Testnet USDC Gas*\n\n"
        f"RPC: `{url}`\n"
        f"Suggested: `{gas['gwei']} gwei`\n"
        f"Base fee: `{gas['base_gwei']} gwei`\n"
        f"Native send (~21k): `{fmt_usdc(gas['native_usdc'])}`\n"
        f"ERC-20 transfer (~50k): `{fmt_usdc(gas['erc20_usdc'])}`\n\n"
        "_Testnet fees, paid in test USDC. Do not read this as ETH._"
    )

HELP_TEXT = (
    "⚡ *ARC Testnet Sentinel*\n\n"
    "/status — nodes, block, txs, latency, USDC gas\n"
    "/gas — live USDC fee estimate\n"
    "/drift — block gap across RPCs\n"
    "/chain — chain id check (5042002)\n"
    "/peers — net peer count\n"
    "/txs — latest block transaction count\n"
    "/bal 0x... — native USDC balance\n"
    "/watch 0x... — balance watch\n"
    "/unwatch 0x... — remove watch\n"
    "/sla — 24h uptime\n"
    "/help — this menu"
)

# ==========================================
# MONITOR
# ==========================================
def monitor_worker():
    log_msg("Testnet Sentinel Core & SQLite initialized.")
    while True:
        online_blocks = []
        for url in list(ACTIVE_RPC_POOL):
            broken_until = rpc_status[url]["circuit_broken_until"]
            if broken_until and time.time() < broken_until:
                continue
            start_time = time.time()
            try:
                block_hex = rpc_call(url, "eth_blockNumber")
                latency = int((time.time() - start_time) * 1000)
                block_height = int(block_hex, 16)
                gas = {}
                txs = 0
                try:
                    gas = fetch_gas(url)
                except Exception as ge:
                    log_msg(f"[!] Gas read failed {url}: {ge}")
                try:
                    txs = fetch_tx_count(url)
                except Exception as te:
                    log_msg(f"[!] Tx count failed {url}: {te}")
                log_msg(
                    f"🟢 [ONLINE] {url} | Block: {block_height} | Txs: {txs} | "
                    f"Ping: {latency}ms | Gas: {gas.get('gwei')} gwei"
                )
                if rpc_status[url]["alerted"]:
                    send_telegram_alert(
                        f"✅ *Testnet node recovered*\n`{url}` is ONLINE at block `{block_height}` "
                        f"({txs} txs)."
                    )
                rpc_status[url]["failures"] = 0
                rpc_status[url]["alerted"] = False
                rpc_status[url]["circuit_broken_until"] = 0
                global_node_data[url] = {
                    "status": "ONLINE",
                    "latency": latency,
                    "block": block_height,
                    "txs": txs,
                    "gas": gas,
                }
                log_to_db(url, "ONLINE", latency, block_height)
                online_blocks.append(block_height)
                if gas.get("gwei", 0) >= GAS_ALERT_THRESHOLD_GWEI:
                    send_telegram_alert(
                        f"💸 *High USDC gas (testnet)*\n`{url}` at `{gas['gwei']} gwei` "
                        f"(ERC-20 ~ {fmt_usdc(gas.get('erc20_usdc'))})"
                    )
                if latency >= HIGH_LATENCY_THRESHOLD_MS:
                    send_telegram_alert(f"🐢 *High latency*\n`{url}` ping `{latency}ms`")
            except Exception as e:
                latency = int((time.time() - start_time) * 1000)
                log_msg(f"🔴 [OFFLINE] {url} | Error: {e}")
                rpc_status[url]["failures"] += 1
                global_node_data[url] = {
                    "status": "OFFLINE",
                    "latency": latency,
                    "block": 0,
                    "txs": 0,
                    "gas": {},
                }
                log_to_db(url, "OFFLINE", latency, 0)
                if rpc_status[url]["failures"] >= FAILURE_THRESHOLD and not rpc_status[url]["alerted"]:
                    rpc_status[url]["alerted"] = True
                    rpc_status[url]["circuit_broken_until"] = time.time() + COOLDOWN_SECONDS
                    send_telegram_alert(
                        f"⚠️ *ARC testnet node down*\n`{url}` failed {FAILURE_THRESHOLD} times. "
                        f"Cooling off {COOLDOWN_SECONDS}s."
                    )
        if len(online_blocks) >= 2:
            drift = max(online_blocks) - min(online_blocks)
            if drift > MAX_DRIFT_THRESHOLD:
                send_telegram_alert(f"📏 *Block drift*\nGap `{drift}` blocks across testnet RPCs.")
        for address in list(watch_addresses):
            try:
                bal = fetch_balance(healthy_url(), address)
                log_msg(f"👀 watch {address} = {bal:.6f} USDC")
            except Exception as e:
                log_msg(f"[!] watch failed {address}: {e}")
        time.sleep(POLL_SECONDS)

# ==========================================
# TELEGRAM LISTENER
# ==========================================
def handle_command(chat_id, text):
    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    if cmd in ("/start", "/help"):
        send_custom_message(chat_id, HELP_TEXT)
    elif cmd == "/status":
        send_custom_message(chat_id, get_status_report())
    elif cmd == "/gas":
        send_custom_message(chat_id, get_gas_report())
    elif cmd == "/drift":
        blocks = [d.get("block", 0) for d in global_node_data.values() if d.get("status") == "ONLINE"]
        if len(blocks) < 2:
            send_custom_message(chat_id, "Not enough online nodes for drift.")
        else:
            send_custom_message(chat_id, f"📏 Drift: `{max(blocks) - min(blocks)}` blocks")
    elif cmd == "/chain":
        try:
            cid = int(rpc_call(healthy_url(), "eth_chainId"), 16)
            ok = "✅" if cid == EXPECTED_CHAIN_ID else "❌"
            send_custom_message(chat_id, f"{ok} Chain ID `{cid}` (expected `{EXPECTED_CHAIN_ID}`)")
        except Exception as e:
            send_custom_message(chat_id, f"Chain check failed: `{e}`")
    elif cmd == "/peers":
        try:
            peers = int(rpc_call(healthy_url(), "net_peerCount"), 16)
            send_custom_message(chat_id, f"🔗 Peers: `{peers}`")
        except Exception as e:
            send_custom_message(chat_id, f"Peer count failed: `{e}`")
    elif cmd == "/txs":
        try:
            url = healthy_url()
            txs = fetch_tx_count(url)
            block = global_node_data.get(url, {}).get("block", 0)
            send_custom_message(
                chat_id,
                f"🧾 Latest block `{block}` on `{url}` has `{txs}` transactions.",
            )
        except Exception as e:
            send_custom_message(chat_id, f"Tx count failed: `{e}`")
    elif cmd == "/bal" and len(parts) > 1:
        address = parts[1]
        try:
            bal = fetch_balance(healthy_url(), address)
            send_custom_message(chat_id, f"💰 `{address}`\n`{bal:.6f} USDC`")
        except Exception as e:
            send_custom_message(chat_id, f"Balance failed: `{e}`")
    elif cmd == "/watch" and len(parts) > 1:
        address = parts[1]
        if address not in watch_addresses:
            watch_addresses.append(address)
        send_custom_message(chat_id, f"👀 Watching `{address}`")
    elif cmd == "/unwatch" and len(parts) > 1:
        address = parts[1]
        if address in watch_addresses:
            watch_addresses.remove(address)
        send_custom_message(chat_id, f"Removed `{address}`")
    elif cmd == "/sla":
        sla = sla_percent(24)
        send_custom_message(chat_id, f"📈 24h SLA: `{sla}%`" if sla is not None else "No SLA samples yet.")
    else:
        send_custom_message(chat_id, "Unknown command. /help")

def telegram_listener():
    offset = 0
    while True:
        if not TELEGRAM_BOT_TOKEN:
            time.sleep(5)
            continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=30"
            resp = requests.get(url, timeout=35)
            if resp.status_code == 200:
                data = resp.json()
                for result in data.get("result", []):
                    offset = result["update_id"] + 1
                    if "callback_query" in result:
                        cq = result["callback_query"]
                        chat_id = cq.get("message", {}).get("chat", {}).get("id")
                        text = cq.get("data", "")
                        if chat_id and text:
                            handle_command(chat_id, text)
                        continue
                    message = result.get("message", {})
                    text = message.get("text", "").strip()
                    chat_id = message.get("chat", {}).get("id")
                    if chat_id and text:
                        handle_command(chat_id, text)
        except Exception as e:
            log_msg(f"[!] Telegram listener: {e}")
            time.sleep(5)
        time.sleep(1)

threading.Thread(target=monitor_worker, daemon=True).start()
threading.Thread(target=telegram_listener, daemon=True).start()

# ==========================================
# DASHBOARD
# ==========================================
@app.route("/")
def index():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ARC Testnet Sentinel</title>
        <style>
            body { background:#0b0f14; color:#d6ffe8; font-family: ui-monospace, monospace; padding: 24px; }
            h2 { color:#7CFFB2; border-bottom: 1px solid #1e3a2f; padding-bottom: 8px; }
            .muted { color:#8aa; }
            pre { white-space: pre-wrap; word-wrap: break-word; font-size: 13px; line-height: 1.45; }
        </style>
    </head>
    <body>
        <h2>ARC Testnet Sentinel</h2>
        <p class="muted">USDC gas · txs · drift · SLA · chain 5042002</p>
        <pre id="logs">Booting core modules...</pre>
        <script>
            const evtSource = new EventSource("/stream");
            evtSource.onmessage = function(event) {
                const logPre = document.getElementById("logs");
                logPre.textContent += "\\n" + event.data;
                window.scrollTo(0, document.body.scrollHeight);
            };
        </script>
    </body>
    </html>
    """
    return html

@app.route("/stream")
def stream():
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=10)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield "data: [ heartbeat ]\n\n"
    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

@app.route("/api/status")
def api_status():
    return jsonify({
        "network": "arc-testnet",
        "chain_id": EXPECTED_CHAIN_ID,
        "nodes": global_node_data,
        "sla_24h": sla_percent(24),
        "watch": watch_addresses,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
