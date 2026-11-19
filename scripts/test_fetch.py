import time
import httpx
import pandas as pd
import numpy as np

def test_fetch():
    print("Testing Binance fetch...")
    url = "https://data-api.binance.vision/api/v3/klines"
    r = httpx.get(url, params={"symbol": "BTCUSDT", "interval": "5m", "limit": 10})
    print("Fetched:", len(r.json()))

if __name__ == "__main__":
    test_fetch()
