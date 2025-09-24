# High_Level.py
"""
Hi-Level (High_Level.py)
LBank whale detector — هر ران:
 - تریدهای اخیر را می‌گیرد
 - total_quote_5m و max_trade را محاسبه می‌کند
 - اگر تریدی >= threshold (پیش‌فرض 100000 USD) وجود داشت، نهنگ شناسایی می‌شود
 - خروجی رکوردها را در data/data_YYYYMMDDTHHMMSSZ.csv ذخیره و به data/data.csv الصاق می‌کند
 - لاگ در logs/run_log.txt
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
        # avoid adding duplicate handlers on repeated imports/runs
        logger.remove()
    except Exception:
        pass
    logger.add(LOG_FILE, rotation="1 MB", retention="7 days", enqueue=True)
else:
    # ensure file handler exists for stdlib logger
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(LOG_FILE) for h in logger.handlers):
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(formatter)
        logger.addHandler(fh)


# ---------- Configuration ----------
LBANK_TRADES = "https://api.lbank.info/v2/trades.do"
DEFAULT_SYMBOLS = ["btc_usdt", "eth_usdt", "bnb_usdt"]
DEFAULT_THRESHOLD_USD = 100_000  # آستانه‌ی ترید "نهنگ" برای ترید منفرد
TRADES_SIZE = 500  # تعداد تریدهایی که از API می‌گیریم (تا عمق کافی داشته باشیم)
REQUEST_TIMEOUT = 15  # ثانیه
# -----------------------------------


def get_trades(symbol: str = "btc_usdt", size: int = TRADES_SIZE):
    """دریافت لیست تریدهای اخیر از LBank (return list of dict)"""
    try:
        resp = requests.get(LBANK_TRADES, params={"symbol": symbol, "size": size}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        # LBank پاسخ structure: {"code":0,"message":"OK","data":[...]} یا {"data":[...]}
        if isinstance(payload, dict):
            data = payload.get("data", [])
        else:
            data = payload
        return data or []
    except Exception as e:
        logger.error(f"Error fetching trades for {symbol}: {e}")
        logger.debug(traceback.format_exc())
        return []


def detect_whale(symbol: str = "btc_usdt", threshold_usd: float = DEFAULT_THRESHOLD_USD):
    """
    تحلیل تریدها و تشخیص نهنگ
    خروجی: dict با کلیدهای symbol, timestamp_utc, price_last, total_quote_5m, max_trade, whale_detected
    """
    timestamp = datetime.utcnow().isoformat()
    try:
        trades = get_trades(symbol)
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
        # expected fields: price, amount, type, date (but be defensive)
        if "price" not in df.columns or "amount" not in df.columns:
            logger.warning(f"Unexpected trade schema for {symbol}, columns: {list(df.columns)}")
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

        whale_trades = df[df["quote"] >= threshold_usd]
        whale_flag = not whale_trades.empty

        price_last = float(df["price"].iloc[-1]) if len(df) > 0 else None

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
        return {
            "symbol": symbol,
            "timestamp_utc": timestamp,
            "price_last": None,
            "total_quote_5m": 0.0,
            "max_trade": 0.0,
            "whale_detected": False,
        }


def ensure_gitkeep(data_dir: str = "data"):
    """اگر .gitkeep وجود نداره بساز"""
    try:
        os.makedirs(data_dir, exist_ok=True)
        gitkeep = os.path.join(data_dir, ".gitkeep")
        if not os.path.exists(gitkeep):
            with open(gitkeep, "w") as f:
                f.write("")  # فقط ایجاد فایل خالی
            logger.info(f"Created {gitkeep}")
    except Exception as e:
        logger.warning(f"Could not create .gitkeep: {e}")


def save_to_csv(records, path_cumulative: str = "data/data.csv"):
    """
    ذخیره‌ی رکوردهای جاری:
     - یک فایل run-specific با timestamp می‌سازد: data/data_YYYYMMDDTHHMMSSZ.csv
     - رکوردها را به فایل تجمعی data/data.csv اضافه می‌کند
    """
    try:
        os.makedirs("data", exist_ok=True)
        ensure_gitkeep("data")

        df_new = pd.DataFrame(records)

        # per-run filename
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        run_filename = f"data/data_{ts}.csv"
        try:
            df_new.to_csv(run_filename, index=False)
            logger.info(f"Saved run file: {run_filename}")
            print(f"RUN_SAVED:{run_filename}")
        except Exception as e:
            logger.error(f"Failed to save run file {run_filename}: {e}")
            logger.debug(traceback.format_exc())

        # append/update cumulative file
        try:
            if os.path.exists(path_cumulative):
                try:
                    df_old = pd.read_csv(path_cumulative)
                    # avoid duplicate columns/order issues — concat straightforwardly
                    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
                except Exception:
                    # if reading old file fails, replace it
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
            # print a machine-parseable marker for workflow logs
            print(f"DATA_SAVED:{path_cumulative}:{size}")
        except Exception as e:
            logger.error(f"Failed to update cumulative {path_cumulative}: {e}")
            logger.debug(traceback.format_exc())

    except Exception as e:
        logger.error(f"Unexpected error in save_to_csv: {e}")
        logger.debug(traceback.format_exc())


def main():
    # symbols و threshold را می‌توان از environment نیز خواند (اختیاری)
    symbols_env = os.getenv("HIGH_LEVEL_SYMBOLS")
    threshold_env = os.getenv("HIGH_LEVEL_THRESHOLD_USD")

    symbols = [s.strip() for s in symbols_env.split(",")] if symbols_env else DEFAULT_SYMBOLS
    try:
        threshold = float(threshold_env) if threshold_env else DEFAULT_THRESHOLD_USD
    except Exception:
        threshold = DEFAULT_THRESHOLD_USD

    logger.info(f"Starting High_Level run for symbols={symbols} threshold={threshold}")
    results = []
    for sym in symbols:
        res = detect_whale(sym, threshold_usd=threshold)
        results.append(res)
        if res.get("whale_detected"):
            logger.warning(f"🐋 Whale detected in {sym}: max_trade={res.get('max_trade')}")
    save_to_csv(results)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error in High_Level: {e}")
        logger.debug(traceback.format_exc())
        # bubble up non-zero exit so CI/Workflow knows it failed
        raise
