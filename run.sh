#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "========================================================"
echo "⚡ 啟動 5m 超買超賣反彈 & 馬丁格爾量化通知系統"
echo "📊 監控標的: BTCUSDT / ETHUSDT / SOLUSDT (Binance 5m)"
echo "🌐 控制台網址: http://127.0.0.1:8088"
echo "========================================================"

exec ./.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8088
