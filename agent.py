import asyncio
import time
import aiohttp
import os
import threading
import queue
import sqlite3
import requests
from flask import Flask, Response, jsonify, request

# =======================================================================
# CONFIGURATION & GLOBAL STATE (TESTNET)
# =======================================================================
PRIMARY_RPC_ENDPOINTS = [
    "https://arc-testnet.drpc.org",          
    "https://rpc.testnet.arc.network"        
]
FALLBACK_RPC_ENDPOINTS = [
    "https://testnet.arc.network"
]

DISCORD_WEBHOOK_URL = "YOUR_DISCORD_WEBHOOK_URL_HERE"
TELEGRAM_BOT_TOKEN = "8957473691:AAG6BgDwvyejUpEscgs9qcnGT-ddgtvrlEA" 
TELEGRAM_CHAT_ID = "8822300532"

FAILURE_THRESHOLD = 3
COOLDOWN_SECONDS = 20
MAX_DRIFT_THRESHOLD = 8
SUPER_PATIENT_TIMEOUT = 6   
GAS_ALERT_THRESHOLD_GWEI = 150.0  
HIGH_LATENCY_THRESHOLD_MS = 500 

DB_FILE = "arc_testnet_sla.db"
ACTIVE_RPC_POOL = list(PRIMARY_RPC_ENDPOINTS)
rpc_status = {url: {"failures": 0, "circuit_broken_until": 0} for url in (PRIMARY_RPC_ENDPOINTS + FALLBACK_RPC_ENDPOINTS)}
global_node_data = {}  

app = Flask(__name__)
log_queue = queue.Queue(maxsize=200)

# =======================================================================
# SQLITE DATABASE SETUP (SLA & UPTIME)
# =======================================================================
def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS uptime_logs
                     (timestamp TEXT, node_url TEXT, status TEXT, latency_ms INTEGER, block_height INTEGER)''')
        conn.commit()
        conn.close()
    except Exception:
        pass

def log_to_db(url, status, latency=0, block=0):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        c.execute("INSERT INTO uptime_logs VALUES (?, ?, ?, ?, ?)", (timestamp, url, status, latency, block))
        conn.commit()
        conn.close()
    except Exception:
        pass

# =======================================================================
# CORE LOGGING & ALERTS
# =======================================================================
def emit_log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    formatted_msg = f"{timestamp} | {message}\n"
    try:
        log_queue.put_nowait(formatted_msg)
    except queue.Full:
        try:
            log_queue.get_nowait()
            log_queue.put_nowait(formatted_msg)
        except Exception:
            pass

async def send_discord_alert(session, alert_title, details):
    if DISCORD_WEBHOOK_URL == "YOUR_DISCORD_WEBHOOK_URL_HERE": return
    payload = {"username": "Arc Testnet Sentinel", "content": f"🚨 **[{alert_title}]**\n{details}\n⏰ **Time:** {time.strftime('%Y-%m-%d %H:%M:%S')}"}
    try: await session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
    except Exception: pass

def send_telegram_message(text):
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE": return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try: requests.post(url, json=payload, timeout=5)
    except Exception: pass

# =======================================================================
# ASYNC NETWORK MONITORING ENGINE
# =======================================================================
async def fetch_json(session, url, payload):
    start_time = time.time()
    try:
        async with session.post(url, json=payload, timeout=SUPER_PATIENT_TIMEOUT) as response:
            if response.status == 200:
                data = await response.json()
                latency = int((time.time() - start_time) * 1000)
                return data, latency
    except Exception:
        pass
    return None, 0

async def check_rpc_with_circuit_breaker(session, url):
    current_time = time.time()
    if current_time < rpc_status[url]["circuit_broken_until"]: return None

    block_payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["latest", False], "id": 1}
    gas_payload = {"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 2}
    
    try:
        res_block_tuple, res_gas_tuple = await asyncio.gather(
            fetch_json(session, url, block_payload),
            fetch_json(session, url, gas_payload),
            return_exceptions=True
        )
        
        if isinstance(res_block_tuple, tuple) and isinstance(res_gas_tuple, tuple):
            res_block, latency_block = res_block_tuple
            res_gas, _ = res_gas_tuple
            
            if res_block and res_gas:
                block_data = res_block.get("result")
                gas_hex = res_gas.get("result")
                
                if block_data and "number" in block_data and gas_hex:
                    rpc_status[url]["failures"] = 0  
                    gas_gwei = round(int(gas_hex, 16) / 10**9, 2) 
                    height = int(block_data["number"], 16)
                    
                    log_to_db(url, "ONLINE", latency_block, height)
                    return {"url": url, "height": height, "hash": block_data["hash"], "gas": gas_gwei, "latency": latency_block}
    except Exception:
        pass

    rpc_status[url]["failures"] += 1
    log_to_db(url, "OFFLINE", 0, 0)
    
    if rpc_status[url]["failures"] >= FAILURE_THRESHOLD:
        rpc_status[url]["circuit_broken_until"] = current_time + COOLDOWN_SECONDS
        emit_log(f"🔴 [CRITICAL] Testnet Node isolated: {url}")
        await send_discord_alert(session, "NODE_CRASH_ALERT", f"🔴 Testnet Node Down: {url}")
    return None

async def monitor_network():
    global ACTIVE_RPC_POOL, global_node_data
    emit_log("INFO | Testnet Sentinel Core & SQLite initialized successfully.")
    
    connector = aiohttp.TCPConnector(limit_per_host=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                tasks = [check_rpc_with_circuit_breaker(session, url) for url in ACTIVE_RPC_POOL]
                results = await asyncio.gather(*tasks)
                
                latest_data = {res["url"]: res for res in results if res is not None}
                global_node_data = latest_data 
                
                if not latest_data:
                    emit_log("⚠️ [FATAL] All Testnet nodes offline. Engaging failover array.")
                    ACTIVE_RPC_POOL = list(PRIMARY_RPC_ENDPOINTS + FALLBACK_RPC_ENDPOINTS)
                    await asyncio.sleep(5)
                    continue
                
                for url, data in latest_data.items():
                    perf_indicator = "🟢" if data['latency'] < HIGH_LATENCY_THRESHOLD_MS else "🟠"
                    emit_log(f"INFO | {perf_indicator} [ONLINE] {url} | Block: {data['height']} | Ping: {data['latency']}ms | Gas: {data['gas']} Gwei")
                
                await asyncio.sleep(15)
                
            except Exception as e:
                emit_log(f"🔴 [ERROR] Loop exception: {str(e)}")
                await asyncio.sleep(5)

# =======================================================================
# TELEGRAM BOT POLLING (BACKGROUND THREAD)
# =======================================================================
def telegram_polling():
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE": return
    
    # Clear any active webhooks to prevent 409 conflicts
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=5)
    except Exception:
        pass

    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=5"
            resp = requests.get(url, timeout=10).json()
            if resp.get("ok"):
                for update in resp["result"]:
                    last_update_id = update["update_id"]
                    msg = update.get("message", {}).get("text", "").strip().lower()
                    if msg in ["/status", "/start"]:
                        status_text = "⚡ **ARC Testnet Status**\n\n"
                        if global_node_data:
                            for node, data in global_node_data.items():
                                status_text += f"🔗 {node}\n📦 Block: {data['height']}\n⏱ Ping: {data['latency']}ms\n\n"
                        else:
                            status_text += "⚠️ Nodes initializing or temporarily offline.\n"
                        send_telegram_message(status_text)
            else:
                emit_log(f"⚠️ Telegram API Warning: {resp}")
        except Exception as e: 
            emit_log(f"🔴 Telegram Polling Error: {str(e)}")
        time.sleep(3)

def start_background_tasks():
    init_db()
    threading.Thread(target=telegram_polling, daemon=True).start()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(monitor_network())
    except Exception:
        pass

# Safe Thread Startup
if not any(t.name == "TestnetSentinelBackgroundThread" for t in threading.enumerate()):
    bg_thread = threading.Thread(target=start_background_tasks, daemon=True, name="TestnetSentinelBackgroundThread")
    bg_thread.start()

# =======================================================================
# FLASK WEB SERVER & API ROUTES
# =======================================================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ARC Testnet Sentinel</title>
    <style>
        body { background-color: #050505; color: #ffcc00; font-family: 'Courier New', monospace; padding: 20px; font-size: 14px;}
        .header { border-bottom: 1px solid #ffcc00; padding-bottom: 10px; margin-bottom: 15px;}
        #console { white-space: pre-wrap; line-height: 1.5; color: #ffffff;}
    </style>
</head>
<body>
    <div class="header">
        ⚡ ARC TESTNET SENTINEL INFRASTRUCTURE v3.1<br>
        SYSTEM: API ACTIVE | PROXY LOAD BALANCER ACTIVE | DATABASE CONNECTED<br>
    </div>
    <div id="console">Booting core modules...<br></div>
    <script>
        const consoleDiv = document.getElementById('console');
        const eventSource = new EventSource('/stream');
        eventSource.onmessage = function(event) {
            consoleDiv.innerHTML += event.data + "<br>";
            window.scrollTo(0, document.body.scrollHeight);
        };
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return HTML_TEMPLATE

@app.route('/stream')
def stream():
    def generate_logs():
        yield "data: System connected to live stream.\n\n"
        while True:
            try:
                msg = log_queue.get(timeout=5)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield "data: \n\n"
            except Exception:
                break
    return Response(generate_logs(), mimetype='text/event-stream')

@app.route('/api/health', methods=['GET'])
def api_health():
    return jsonify({
        "network": "Arc Testnet",
        "timestamp": time.time(),
        "active_nodes": len(global_node_data),
        "nodes": global_node_data
    })

@app.route('/rpc', methods=['POST'])
def proxy_balancer():
    if not global_node_data:
        return jsonify({"error": "No healthy testnet nodes available"}), 503
    best_node = min(global_node_data.values(), key=lambda x: x['latency'])
    try:
        resp = requests.post(best_node['url'], json=request.json, timeout=5)
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        return jsonify({"error": "Proxy routing failed", "details": str(e)}), 502

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
