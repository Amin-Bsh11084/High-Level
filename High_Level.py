# High_Level.py
"""
High_Level — LBank whale detector (debug-enabled)
"""

import os
import sys
import traceback
from datetime import datetime
import requests
import pandas as pd

# try to use loguru, fallback to stdlib logging if not installed
try:
    from loguru import logger  # type: ignore
    _HAS_LOGURU = True
except Exception:
    _HAS_LOGURU = False
    import logging
    logger = logging.getLogger("High_Level")
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# create logs folder and attach file logging
os.makedirs("logs", exist_ok=True)
LOG_FILE = "logs/run_log.txt"
if _HAS_LOGURU:
    try:
        logger.remove()  # avoid duplicate handlers on repeated runs in same process
    except Exception:
        pass
    logger.add(LOG_FILE, rotation="1 MB", retention="7 days", enqueue=True)
else:
    import logging as _logging
    # ensure file handler exists for stdlib logger
    if not any(isinstance(h, _logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(LOG_FILE) for h in logger.handlers):
        fh = _logging.FileHandler(LOG_FILE)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

# ---------- Configuration ----------
LBANK_TRADES = "https://api.lbank.info/v2/trades.do"
DEFAULT_SYMBOLS = ["btc_usdt", "eth_usdt", "bnb_usdt"]
DEFAULT_THRESHOLD_USD = 100_000
TRADES_SIZE = 500
REQUEST_TIMEOUT = 15
# -----------------------------------

def dbg_print(msg):
    # helper to always print debug to stdout (so Actions log می‌گیره)
    try:
        print(f"DBG: {msg}", flush=True)
    except Exception:
        pass

dbg_print(f"High_Level.py start (pid={os.getpid()}, cwd={os.getcwd()})")

def get_trades(symbol: str = "btc_usdt", size: int = TRADES_SIZE):
    dbg_print(f"get_trades() called for symbol={symbol} size={size}")
    try:
        resp = requests.get(LBANK_TRADES, params={"symbol": symbol, "size": size}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict):
            data = payload.get("data", [])
        else:
            data = payload
        dbg_print(f"get_trades() for {symbol} returned {len(data) if data is not None else 0} items")
        return data or []
    except Exception as e:
        logger.error(f"Error fetching trades for {symbol}: {e}")
        logger.debug(traceback.format_exc())
        dbg_print(f"get_trades() EXCEPTION for {symbol}: {e}")
        return []

def detect_whale(symbol: str = "btc_usdt", threshold_usd: float = DEFAULT_THRESHOLD_USD):
    timestamp = datetime.utcnow().isoformat()
    dbg_print(f"detect_whale() start for {symbol}")
    try:
        trades = get_trades(symbol)
        dbg_print(f"{symbol} - trades fetched: {len(trades)}")
        if not trades:
            logger.info(f"No trades returned for {symbol}")
            return {
                "symbol": symbol,
                "timestamp_utc": timestamp,
                "price_last": None,
                "total_quote_5m": 0.0,
                "max_trade": 0.0,
                "whale_detected": False,
            }

        df = pd.DataFrame(trades)
        if "price" not in df.columns or "amount" not in df.columns:
            logger.warning(f"Unexpected trade schema for {symbol}, columns: {list(df.columns)}")
            dbg_print(f"{symbol} - unexpected schema: {list(df.columns)}")
            return {
                "symbol": symbol,
                "timestamp_utc": timestamp,
                "price_last": None,
                "total_quote_5m": 0.0,
                "max_trade": 0.0,
                "whale_detected": False,
            }

        df["price"] = df["price"].astype(float)
        df["amount"] = df["amount"].astype(float)
        df["quote"] = df["price"] * df["amount"]

        total_5m = float(df["quote"].sum())
        max_trade = float(df["quote"].max())
        price_last = float(df["price"].iloc[-1]) if len(df) > 0 else None

        dbg_print(f"{symbol} - total_5m={total_5m:.2f} max_trade={max_trade:.2f} price_last={price_last}")
        whale_trades = df[df["quote"] >= threshold_usd]
        whale_flag = not whale_trades.empty

        logger.info(f"{symbol} | total_quote_5m={total_5m:.2f} | max_trade={max_trade:.2f} | whale={whale_flag}")
        return {
            "symbol": symbol,
            "timestamp_utc": timestamp,
            "price_last": price_last,
            "total_quote_5m": total_5m,
            "max_trade": max_trade,
            "whale_detected": bool(whale_flag),
        }

    except Exception as e:
        logger.error(f"Exception in detect_whale for {symbol}: {e}")
        logger.debug(traceback.format_exc())
        dbg_print(f"EXCEPTION in detect_whale for {symbol}: {e}")
        return {
            "symbol": symbol,
            "timestamp_utc": timestamp,
            "price_last": None,
            "total_quote_5m": 0.0,
            "max_trade": 0.0,
            "whale_detected": False,
        }

def ensure_gitkeep(data_dir: str = "data"):
    try:
        os.makedirs(data_dir, exist_ok=True)
        gitkeep = os.path.join(data_dir, ".gitkeep")
        if not os.path.exists(gitkeep):
            with open(gitkeep, "w") as f:
                f.write("")
            logger.info(f"Created {gitkeep}")
            dbg_print(f"Created {gitkeep}")
    except Exception as e:
        logger.warning(f"Could not create .gitkeep: {e}")
        dbg_print(f"Could not create .gitkeep: {e}")

def save_to_csv(records, path_cumulative: str = "data/data.csv"):
    dbg_print("save_to_csv() called")
    try:
        os.makedirs("data", exist_ok=True)
        ensure_gitkeep("data")

        df_new = pd.DataFrame(records)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        run_filename = f"data/data_{ts}.csv"

        try:
            df_new.to_csv(run_filename, index=False)
            logger.info(f"Saved run file: {run_filename}")
            dbg_print(f"RUN_SAVED:{run_filename}")
            print(f"RUN_SAVED:{run_filename}", flush=True)
        except Exception as e:
            logger.error(f"Failed to save run file {run_filename}: {e}")
            logger.debug(traceback.format_exc())
            dbg_print(f"Failed to save run file: {e}")

        # update cumulative
        try:
            if os.path.exists(path_cumulative):
                try:
                    df_old = pd.read_csv(path_cumulative)
                    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
                except Exception:
                    logger.warning(f"Could not read existing cumulative {path_cumulative}, overwriting.")
                    df_final = df_new
            else:
                df_final = df_new

            df_final.to_csv(path_cumulative, index=False)
            size = None
            try:
                size = os.path.getsize(path_cumulative)
            except Exception:
                pass
            logger.info(f"Cumulative saved to {path_cumulative} (size={size})")
            dbg_print(f"DATA_SAVED:{path_cumulative}:{size}")
            print(f"DATA_SAVED:{path_cumulative}:{size}", flush=True)
        except Exception as e:
            logger.error(f"Failed to update cumulative {path_cumulative}: {e}")
            logger.debug(traceback.format_exc())
            dbg_print(f"Failed to update cumulative: {e}")

    except Exception as e:
        logger.error(f"Unexpected error in save_to_csv: {e}")
        logger.debug(traceback.format_exc())
        dbg_print(f"Unexpected error in save_to_csv: {e}")

def main():
    dbg_print("main() start")
    symbols_env = os.getenv("HIGH_LEVEL_SYMBOLS")
    threshold_env = os.getenv("HIGH_LEVEL_THRESHOLD_USD")

    symbols = [s.strip() for s in symbols_env.split(",")] if symbols_env else DEFAULT_SYMBOLS
    try:
        threshold = float(threshold_env) if threshold_env else DEFAULT_THRESHOLD_USD
    except Exception:
        threshold = DEFAULT_THRESHOLD_USD

    dbg_print(f"Starting run for symbols={symbols} threshold={threshold}")
    results = []
    for sym in symbols:
        dbg_print(f"Calling detect_whale for {sym}")
        res = detect_whale(sym, threshold_usd=threshold)
        dbg_print(f"Result for {sym}: {res}")
        results.append(res)
        if res.get("whale_detected"):
            logger.warning(f"🐋 Whale detected in {sym}: max_trade={res.get('max_trade')}")

    dbg_print(f"ABOUT_TO_SAVE cumulative, records_count={len(results)}")
    save_to_csv(results)
    dbg_print("main() finished")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error in High_Level: {e}")
        logger.debug(traceback.format_exc())
        dbg_print(f"FATAL: {e}")
        raise
