# ARC Testnet Sentinel

Uptime, latency, block drift, and USDC gas monitor for Arc testnet RPC endpoints, with a Telegram bot and a live log dashboard.

Arc prices gas in USDC (18 decimals), not a volatile native token. This service polls public testnet RPCs, records SLA in SQLite, and alerts when a node fails, latency spikes, gas rises, or blocks drift apart.

**Network:** Arc testnet · chain ID `5042002`  
**Explorer:** [explorer.testnet.arc.io](https://explorer.testnet.arc.io)

## Live dashboard

Open this in Chrome to run the agent:

**https://arc-testnet-agent-uixx.onrender.com/**

| Endpoint | URL |
| --- | --- |
| Dashboard | https://arc-testnet-agent-uixx.onrender.com/ |
| JSON status | https://arc-testnet-agent-uixx.onrender.com/api/status |
| Log stream | https://arc-testnet-agent-uixx.onrender.com/stream |

The free Render instance sleeps after inactivity. The first request can take up to a minute while the process wakes.

## Telegram bot

Same monitor, over Telegram. Alerts cover node outages, recovery, high latency, high USDC gas, and block drift.

**Bot:** [ArcTestnetMonitorBot](https://t.me/ArcTestnetMonitorBot)

Send `/help` to the bot. The bot token and chat ID are set as environment variables on Render. They are not stored in this repository.

| Command | Description |
| --- | --- |
| `/start`, `/help` | Command menu |
| `/status` | Node status, block, tx count, latency, USDC gas |
| `/gas` | Live USDC fee estimate |
| `/drift` | Block gap across online RPCs |
| `/chain` | Chain ID check (`5042002`) |
| `/peers` | `net_peerCount` |
| `/txs` | Transaction count in the latest block |
| `/bal 0x...` | Native USDC balance |
| `/watch 0x...` | Add an address to the in-memory watch list |
| `/unwatch 0x...` | Remove a watched address |
| `/sla` | 24-hour uptime percent |

Inline buttons on replies call `/status`, `/gas`, and `/sla`.

## Features

- Multi-RPC health checks with a failure threshold and cooldown
- Block height, latest-block transaction count, and latency
- USDC gas estimate for a native send (~21,000 gas) and an ERC-20 transfer (~50,000 gas)
- Block drift alert across online endpoints
- 24-hour SLA from local uptime logs
- Telegram commands for status, gas, balance, and address watch
- Server-sent events dashboard and `GET /api/status`

## Requirements

- Python 3.10 or newer
- Outbound HTTPS to the Arc testnet RPCs and the Telegram Bot API

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install flask requests
