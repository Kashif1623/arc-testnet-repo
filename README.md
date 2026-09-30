# ⚡ ARC Testnet Monitoring Sentinel

A real-time, crash-proof monitoring agent designed to track ARC network Testnet nodes, check block heights, monitor gas prices, track node latency, and provide live updates via a Telegram Bot.

## 🌐 Live Web Dashboard
* **Live Site URL:** https://arc-testnet-agent.onrender.com

## 🚀 Features
* **Real-time RPC Monitoring:** Tracks primary and fallback RPC endpoints with automatic circuit breakers.
* **Telegram Bot Integration:** Send `/start` or `/status` to the bot to get instant live node statistics.
* **Database Logging:** Records node health, uptime, and latency history using SQLite (`arc_testnet_sla.db`).
* **Live Web Dashboard:** Built-in Flask web server with Server-Sent Events (SSE) streaming live logs.
* **Failover Support:** Automatically switches to backup RPC nodes if primary nodes go offline.

## 🛠️ Tech Stack
* **Language:** Python 3
* **Framework:** Flask
* **Server:** Gunicorn
* **Networking:** aiohttp, Requests
* **Database:** SQLite3

## ⚙️ Deployment on Render
1. Connect this repository to a Web Service on Render.
2. **Build Command:** 
   ```bash
   pip install -r requirements.txt
