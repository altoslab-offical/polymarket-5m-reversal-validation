# Polymarket 5 分鐘漲跌盤策略（BTC / ETH / SOL）

內含三個資料夾，各自獨立執行。打包時已移除：.env（私鑰）、資料庫、交易紀錄、Telegram 設定、報告與快取。

## 開始使用
1. 安裝 Python 3.10 以上，在每個資料夾執行 `pip install -r requirements.txt`
2. 把 `.env.example` 複製成 `.env`，填入你自己的錢包與 API 資料（私鑰絕對不要給別人）
3. 先用模擬盤跑（不加 --live）：`python run_monitor.py --bet 10`
4. 確認沒問題、看懂風險後，才考慮用 `--live` 實盤，並設定 `--max-daily-loss`
5. 同一個幣種只能開一個視窗，重複開會造成雙倍下單

## Mac 使用者
- 用「終端機」執行，指令改成 `python3` 和 `pip3`，例如 `python3 run_monitor.py --bet 10`
- 資料夾裡的 start_paper_trade.bat 是 Windows 用的，Mac 不能點兩下執行，請直接在終端機輸入指令
- Mac 睡眠時程式會停止，要長時間執行請在「系統設定」關掉自動睡眠

## 風險提醒
這是實驗性的自動交易程式，會用真錢下單，可能虧損。過去的回測與實盤成績不代表未來。
