# High_Level.py
"""
High_Level — Market-wide taker delta scanner (CoinGlass 4h free-friendly) + Binance fallback
- یونیورس نمادها به‌صورت محکم از Binance (Futures USDT و در صورت لزوم Spot USDT) کشف می‌شود.
- خروجی یونیورس در data/universe.csv ذخیره می‌شود تا قابل بررسی باشد.
- اگر CoinGlass/Binance در دسترس نباشند، DEMO برای تمام یونیورس اجرا می‌شود (نه فقط 3 نماد).
- CSV خروجی: data/high_level_YYYYMMDD_HHMMSS.csv و data/latest.csv
"""

import os
import json
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd

# ----------------------------
# Logging (prefer loguru)
# ----------------------------
_HAS_LOGURU = False
try:
    from loguru import logger  # type: ignore
    _HAS_LOGURU = True
except Exception:
    import logging
    logger = logging.getLogger("High_Level")

LOG_DIR = Path("logs"); LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "run_log.txt"
STATE_DIR = Path(".state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

if _HAS_LOGURU:
    try:
        logger.remove()
    except Exception:
        pass
    logger.add(
        str(LOG_FILE),
        rotation="1 MB",
        retention="7 days",
        enqueue=True,
        backtrace=True,
        diagnose=False,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )
else:
    import logging
    from logging.handlers import RotatingFileHandler
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(); sh.setFormatter(fmt)
    fh = RotatingFileHandler(str(LOG_FILE), maxBytes=1_000_000, backupCount=5); fh.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(sh); logger.addHandler(fh)

logger.info("🚀 High_Level started.")

# ----------------------------
# CONFIG
# ----------------------------
def getenv_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v else default
    except Exception:
        return default

def getenv_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except Exception:
        return default

def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "")
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

CONFIG = {
    # Universe
    "SYMBOLS": [s.strip().upper() for s in os.getenv("HL_SYMBOLS", "").split(",") if s.strip()],
    "MAX_SYMBOLS": getenv_int("HL_MAX_SYMBOLS", 400),     # سقف بزرگتر
    "EXCLUDE_STABLES": getenv_bool("HL_EXCLUDE_STABLES", True),  # حذف استیبل‌کوین‌ها از یونیورس

    # Taker (free-friendly)
    "COINGLASS_KEY": os.getenv("COINGLASS_API_KEY", "").strip(),
    "COINGLASS_BASE": os.getenv("COINGLASS_BASE", "https://open-api-v4.coinglass.com"),
    "TAKER_INTERVAL": os.getenv("HL_TAKER_INTERVAL", "4h"),
    "TAKER_ALERT": getenv_float("HL_TAKER_ALERT", 0.30),

    # Optional LBank (خاموش برای سرعت)
    "ENABLE_LBANK": getenv_bool("HL_ENABLE_LBANK", False),
    "LBANK_BASE": os.getenv("HL_LBANK_BASE", "https://api.lbkex.com"),
    "LOOKBACK_MIN": getenv_int("HL_LOOKBACK_MIN", 10),

    # Scoring (اختیاری)
    "TRADE_USD_MIN": getenv_float("HL_TRADE_USD_MIN", 100_000.0),
    "SWEEP_DEPTH_USD": getenv_float("HL_SWEEP_DEPTH_USD", 150_000.0),
    "COINGLASS_LIQ_MIN": getenv_float("HL_COINGLASS_LIQ_MIN", 200_000.0),
    "W_LARGE_TRADE": getenv_float("HL_W_LARGE_TRADE", 2.0),
    "W_SWEEP": getenv_float("HL_W_SWEEP", 2.0),
    "W_LIQ": getenv_float("HL_W_LIQ", 2.0),
    "W_OI": getenv_float("HL_W_OI", 1.0),
    "W_PCT": getenv_float("HL_W_PCT", 1.0),
    "LABEL_STRONG": getenv_float("HL_LABEL_STRONG", 5.0),
    "LABEL_PROBABLE": getenv_float("HL_LABEL_PROBABLE", 3.0),

    "FORCE_DEMO": getenv_bool("HL_FORCE_DEMO", False),
}
logger.info("📌 CONFIG: " + json.dumps({k: ("***" if (k=="COINGLASS_KEY" and v) else v) for k, v in CONFIG.items()}))

# ----------------------------
# Helpers
# ----------------------------
def now_utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def safe_request_json(method: str, url: str, headers=None, params=None, timeout=20) -> Optional[dict]:
    try:
        r = requests.request(method=method, url=url, headers=headers, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"HTTP error on {url}: {e}")
        return None

def _filter_out_stables(bases: List[str]) -> List[str]:
    if not CONFIG["EXCLUDE_STABLES"]:
        return bases
    # فهرست ساده‌ی استیبل‌های معروف؛ می‌تونی کامل‌ترش کنی
    stables = {"USDT","USDC","FDUSD","TUSD","DAI","BUSD","PYUSD","USDD","EURS","EURT","USTC","GHO"}
    return [b for b in bases if b not in stables]

# ----------------------------
# Universe discovery (محکم + خروجی به CSV)
# ----------------------------
def _binance_futures_bases() -> List[str]:
    info = safe_request_json("GET", "https://fapi.binance.com/fapi/v1/exchangeInfo")
    bases = []
    if info and isinstance(info, dict) and "symbols" in info:
        for s in info["symbols"]:
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
                base = str(s.get("baseAsset","")).upper()
                if base:
                    bases.append(base)
    return sorted(set(bases))

def _binance_spot_bases() -> List[str]:
    info = safe_request_json("GET", "https://api.binance.com/api/v3/exchangeInfo")
    bases = []
    if info and isinstance(info, dict) and "symbols" in info:
        for s in info["symbols"]:
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
                base = str(s.get("baseAsset","")).upper()
                if base:
                    bases.append(base)
    return sorted(set(bases))

def load_universe() -> List[str]:
    """
    ترتیب کشف یونیورس:
      1) HL_SYMBOLS (اگر ست شده)
      2) data/symbols.csv (ستون 'symbol') — می‌تواند BASE یا BASEUSDT باشد
      3) Binance Futures USDT bases (ترجیح)؛ اگر خالی، Binance Spot USDT
    سپس حذف استیبل‌ها (اختیاری) و سقف HL_MAX_SYMBOLS اعمال می‌شود.
    نتیجه در data/universe.csv ذخیره می‌شود.
    """
    # 1) از ENV
    if CONFIG["SYMBOLS"]:
        bases = [s[:-4] if s.endswith("USDT") else s for s in CONFIG["SYMBOLS"]]
        bases = sorted(set(bases))
        logger.info(f"Universe: from HL_SYMBOLS ({len(bases)})")
    else:
        # 2) از فایل محلی
        csv_path = DATA_DIR / "symbols.csv"
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                if "symbol" in df.columns:
                    vals = [str(x).strip().upper() for x in df["symbol"].tolist() if str(x).strip()]
                    bases = [v[:-4] if v.endswith("USDT") else v for v in vals]
                    bases = sorted(set(bases))
                    logger.info(f"Universe: from data/symbols.csv ({len(bases)})")
                else:
                    bases = []
            except Exception as e:
                logger.warning(f"symbols.csv read failed: {e}")
                bases = []
        else:
            bases = []

        # 3) اگر هنوز خالی بود: Binance Futures → Spot
        if not bases:
            f_bases = _binance_futures_bases()
            if f_bases:
                bases = f_bases
                logger.info(f"Universe: from Binance Futures ({len(bases)})")
            else:
                s_bases = _binance_spot_bases()
                if s_bases:
                    bases = s_bases
                    logger.info(f"Universe: from Binance Spot ({len(bases)})")

        if not bases:
            logger.warning("Universe discovery failed — NO INTERNET or API blocked. Using last resort: BTC,ETH,BNB.")
            bases = ["BTC","ETH","BNB"]

    # حذف استیبل‌ها و سقف
    before = len(bases)
    bases = _filter_out_stables(bases)
    after = len(bases)
    if after != before:
        logger.info(f"Filtered stables: {before} -> {after}")

    bases = bases[: CONFIG["MAX_SYMBOLS"]]
    # ذخیره یونیورس
    try:
        pd.DataFrame({"symbol": bases}).to_csv(DATA_DIR / "universe.csv", index=False)
        logger.info(f"📄 Saved universe: data/universe.csv ({len(bases)} symbols)")
    except Exception as e:
        logger.warning(f"Save universe.csv failed: {e}")
    return bases

# ----------------------------
# CoinGlass taker (4h) + Binance fallback
# ----------------------------
def fetch_coinglass_taker_interval(symbol_pair: str, interval: Optional[str]=None) -> Optional[dict]:
    key = CONFIG["COINGLASS_KEY"]
    if not key:
        return None
    interval = interval or CONFIG["TAKER_INTERVAL"]
    headers = {"CG-API-KEY": key, "accept": "application/json"}
    base = CONFIG["COINGLASS_BASE"].rstrip("/")
    endpoints = [
        f"{base}/api/futures/v2/taker-buy-sell-volume/history",
        f"{base}/api/futures/aggregated-taker-buy-sell-volume/history",
        f"{base}/api/futures/taker-buy-sell-volume/history",
    ]
    params_variants = [
        {"pair": symbol_pair, "interval": interval},
        {"symbol": symbol_pair, "interval": interval},
        {"coin": symbol_pair.replace("USDT",""), "interval": interval},
    ]
    for url in endpoints:
        for params in params_variants:
            data = safe_request_json("GET", url, headers=headers, params=params, timeout=20)
            if not data:
                continue
            items = data.get("data") if isinstance(data, dict) and "data" in data else data
            if isinstance(items, list) and len(items) > 0:
                last = items[-1]
                buy = float(last.get("buyVol") or last.get("buy_volume") or last.get("long") or 0.0)
                sell = float(last.get("sellVol") or last.get("sell_volume") or last.get("short") or 0.0)
                ts = int(last.get("timestamp") or last.get("time") or last.get("t") or 0)
                return {"buyVol": buy, "sellVol": sell, "timestamp": ts}
    return None

def compute_taker_delta_with_fallback(base_symbol: str, interval: Optional[str]=None) -> Optional[float]:
    interval = interval or CONFIG["TAKER_INTERVAL"]
    pair = f"{base_symbol}USDT"

    # 1) CoinGlass
    try:
        cg = fetch_coinglass_taker_interval(pair, interval=interval)
        if cg:
            buy, sell = cg["buyVol"], cg["sellVol"]
            denom = buy + sell
            return 0.0 if denom <= 0 else (buy - sell) / denom
    except Exception as e:
        logger.debug(f"CoinGlass delta error {base_symbol}: {e}")

    # 2) Binance fallback
    try:
        url = "https://fapi.binance.com/futures/data/takerBuySellVol"
        params = {"symbol": pair, "period": interval, "limit": 1}
        data = safe_request_json("GET", url, params=params, timeout=15)
        if data and isinstance(data, list) and len(data) > 0:
            last = data[-1]
            buy = float(last.get("buyVol", 0.0))
            sell = float(last.get("sellVol", 0.0))
            denom = buy + sell
            return 0.0 if denom <= 0 else (buy - sell) / denom
    except Exception as e:
        logger.debug(f"Binance fallback delta error {base_symbol}: {e}")

    # 3) No data
    return None

# ----------------------------
# (اختیاری) LBank parts — خاموش برای سرعت
# ----------------------------
def collect_recent_trades_lbank(symbol: str, lookback_min: int) -> List[dict]:
    return []  # intentionally disabled by default for speed

def collect_orderbook_lbank(symbol: str) -> dict:
    return {}

def detect_large_trades(trades: List[dict], price_hint: Optional[float], min_usd: float) -> Tuple[bool, float, str]:
    return (False, 0.0, "")

def detect_orderbook_sweep(orderbook: dict, sweep_depth_usd: float, price_hint: Optional[float]) -> Tuple[bool, float]:
    return (False, 0.0)

def coinglass_signal(m: dict, liq_min: float) -> Tuple[bool, float, float, float, float]:
    return (False, 0.0, 0.0, 0.0, 0.0)

def fuse_score(lt, sw, cg_sig, pct_change: float) -> Tuple[float, str, str]:
    # Minimal scoring off (we focus on taker delta). Keep API stable.
    return (0.0, "neutral", "")

# ----------------------------
# CSV saving
# ----------------------------
def save_output_csv(df: pd.DataFrame) -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = DATA_DIR / f"high_level_{ts}.csv"
    df.to_csv(out_path, index=False)
    df.to_csv(DATA_DIR / "latest.csv", index=False)
    logger.info(f"✅ CSV saved: {out_path}")
    logger.info(f"✅ CSV saved (latest): {DATA_DIR / 'latest.csv'}")
    return str(out_path)

# ----------------------------
# DEMO rows — برای تمام یونیورس
# ----------------------------
def build_demo_rows(symbols: List[str]) -> List[dict]:
    now = now_utc_iso()
    rows = []
    for i, s in enumerate(symbols):
        # الگویی ساده برای پخش مثبت/منفی
        sign = 1 if (i % 4 in (0,1)) else -1
        delta = round(0.05 * sign, 4)  # 5% برای دمو
        rows.append({
            "timestamp_utc": now,
            "symbol": s,
            "taker_delta_interval": delta,
            "taker_delta_alert_interval": abs(delta) >= CONFIG["TAKER_ALERT"],
            "run_file": "",
        })
    return rows

# ----------------------------
# Main
# ----------------------------
def run_pipeline() -> pd.DataFrame:
    bases = load_universe()
    logger.info(f"🧪 Universe size={len(bases)} | Interval={CONFIG['TAKER_INTERVAL']} | Max={CONFIG['MAX_SYMBOLS']}")

    rows: List[dict] = []
    throttle_every = 30
    pause_sec = 0.6

    for idx, sym in enumerate(bases, start=1):
        try:
            delta = compute_taker_delta_with_fallback(sym, interval=CONFIG["TAKER_INTERVAL"])
            alert = (abs(delta) >= CONFIG["TAKER_ALERT"]) if (delta is not None) else False
            if alert:
                logger.info(f"⚠️ ALERT {sym} Δ{CONFIG['TAKER_INTERVAL']}: {delta:.2%}")
            rows.append({
                "timestamp_utc": now_utc_iso(),
                "symbol": sym,
                "taker_delta_interval": delta,
                "taker_delta_alert_interval": alert,
                "run_file": "",
            })
            if idx % throttle_every == 0:
                time.sleep(pause_sec)
        except Exception as e:
            logger.error(f"[{sym}] error: {e}")
            logger.debug(traceback.format_exc())

    # اگر هیچ دیتایی نگرفتیم، دمو برای کل یونیورس
    if not rows or all(r["taker_delta_interval"] is None for r in rows):
        logger.warning("⚠️ No real taker data — switching to DEMO for entire universe.")
        rows = build_demo_rows(bases)

    return pd.DataFrame(rows)

def main() -> int:
    try:
        df = run_pipeline()
        out_path = save_output_csv(df)
        df["run_file"] = out_path
        df.to_csv(DATA_DIR / "latest.csv", index=False)
        logger.info(f"✅ Completed. Rows: {len(df)}")
        logger.info("\n" + df.head(min(20, len(df))).to_string(index=False))
        return 0
    except Exception as e:
        logger.error(f"❌ Fatal: {e}")
        logger.debug(traceback.format_exc())
        try:
            # حداقل خروجی
            pd.DataFrame([{
                "timestamp_utc": now_utc_iso(),
                "symbol": "N/A",
                "taker_delta_interval": None,
                "taker_delta_alert_interval": False,
                "run_file": "",
            }]).to_csv(DATA_DIR / "latest.csv", index=False)
        except Exception:
            pass
        return 1

if __name__ == "__main__":
    code = main()
    print(f"Exit code: {code}")
    raise SystemExit(code)
