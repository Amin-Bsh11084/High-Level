import os
import requests
import pandas as pd
from datetime import datetime
from loguru import logger

# 📜 پیکربندی لاگ
os.makedirs("logs", exist_ok=True)
logger.add("logs/run_log.txt", rotation="1 MB")

# 🌐 API LBank
LBANK_TRADES = "https://api.lbank.info/v2/trades.do"
LBANK_KLINES = "https://api.lbank.info/v2/klines.do"
LBANK_DEPTH = "https://api.lbank.info/v2/depth.do"

def get_trades(symbol="btc_usdt", size=200):
    r = requests.get(LBANK_TRADES, params={"symbol": symbol, "size": size})
    r.raise_for_status()
    return r.json()["data"]

def detect_whale(symbol="btc_usdt"):
    trades = get_trades(symbol)
    df = pd.DataFrame(trades)
    df["price"] = df["price"].astype(float)
    df["amount"] = df["amount"].astype(float)
    df["quote"] = df["price"] * df["amount"]

    total_5m = df["quote"].sum()
    max_trade = df["quote"].max()

    whale_trades = df[df["quote"] > 100_000]  # ✅ آستانه 100k USDT
    whale_flag = not whale_trades.empty

    return {
        "symbol": symbol,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "price_last": float(df["price"].iloc[-1]),
        "total_quote_5m": float(total_5m),
        "max_trade": float(max_trade),
        "whale_detected": whale_flag
    }

def save_to_csv(records, path="data/data.csv"):
    os.makedirs("data", exist_ok=True)
    df_new = pd.DataFrame(records)

    if os.path.exists(path):
        df_old = pd.read_csv(path)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    df_final.to_csv(path, index=False)
    logger.info(f"✅ Data saved to {path}")

if __name__ == "__main__":
    try:
        logger.info("🚀 Starting Hi-Level LBank scan...")
        results = []
        for sym in ["btc_usdt", "eth_usdt", "bnb_usdt"]:  # لیست کوین‌ها
            res = detect_whale(sym)
            results.append(res)
            if res["whale_detected"]:
                logger.warning(f"🐋 Whale detected in {sym}: {res['max_trade']:.0f} USDT trade!")

        save_to_csv(results)
    except Exception as e:
        logger.error(f"❌ Error: {e}")
