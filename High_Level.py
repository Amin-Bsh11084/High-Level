# High_Level.py
import os
import requests
import pandas as pd
from datetime import datetime
# try to use loguru, fallback to stdlib logging if not installed
try:
    from loguru import logger
except Exception:
    import logging
    logger = logging.getLogger("High_Level")
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# 📜 پیکربندی لاگ
os.makedirs("logs", exist_ok=True)
logger.add("logs/run_log.txt", rotation="1 MB") if "loguru" in globals() or "loguru" in str(type(logger)).lower() else None

# 🌐 API LBank
LBANK_TRADES = "https://api.lbank.info/v2/trades.do"

def get_trades(symbol="btc_usdt", size=200):
    r = requests.get(LBANK_TRADES, params={"symbol": symbol, "size": size}, timeout=15)
    r.raise_for_status()
    return r.json().get("data", [])

def detect_whale(symbol="btc_usdt", threshold_usd=100_000):
    trades = get_trades(symbol)
    if not trades:
        return {
            "symbol": symbol,
            "timestamp_utc": datetime.utcnow().isoformat(),
            "price_last": None,
            "total_quote_5m": 0.0,
            "max_trade": 0.0,
            "whale_detected": False
        }

    df = pd.DataFrame(trades)
    df["price"] = df["price"].astype(float)
    df["amount"] = df["amount"].astype(float)
    df["quote"] = df["price"] * df["amount"]

    total_5m = float(df["quote"].sum())
    max_trade = float(df["quote"].max())

    whale_trades = df[df["quote"] >= threshold_usd]
    whale_flag = not whale_trades.empty

    return {
        "symbol": symbol,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "price_last": float(df["price"].iloc[-1]),
        "total_quote_5m": total_5m,
        "max_trade": max_trade,
        "whale_detected": bool(whale_flag)
    }

def save_to_csv(records, path="data/data.csv"):
    os.makedirs("data", exist_ok=True)
    df_new = pd.DataFrame(records)

    if os.path.exists(path):
        try:
            df_old = pd.read_csv(path)
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df_final = df_new
    else:
        df_final = df_new

    df_final.to_csv(path, index=False)
    size = None
    try:
        size = os.path.getsize(path)
    except:
        pass
    logger.info(f"✅ Data saved to {path} (size={size})")
    print(f"DATA_SAVED:{path}:{size}")

if __name__ == "__main__":
    try:
        logger.info("🚀 Starting High_Level LBank scan...")
        symbols = ["btc_usdt", "eth_usdt", "bnb_usdt"]  # قابل تغییر
        results = []
        for sym in symbols:
            res = detect_whale(sym)
            results.append(res)
            if res["whale_detected"]:
                logger.warning(f"🐋 Whale detected in {sym}: {res['max_trade']:.0f} USDT trade!")
        save_to_csv(results)
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise
