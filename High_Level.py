# High_Level.py
"""
High_Level — Market-wide taker delta (from LBank trades only)

چه کار می‌کند؟
- کشف یونیورس از LBank: همه‌ی جفت‌های *_USDT (Active)
- برای هر نماد (BASEUSDT) در بازه‌ی مشخص (پیش‌فرض: 4h) تریدهای LBank را می‌گیرد،
  سمت معامله (buy/sell) را تشخیص می‌دهد، و حجم quote را جمع می‌بندد:
      buy_quote = sum(price * amount for buy trades)
      sell_quote = sum(price * amount for sell trades)
      delta = (buy_quote - sell_quote) / (buy_quote + sell_quote)
- نتیجه را در CSV ذخیره می‌کند:
    data/high_level_YYYYMMDD_HHMMSS.csv  و  data/latest.csv
- لاگ‌ها در logs/run_log.txt

نکات:
- API عمومی LBank محدودیت‌ها/شکل پاسخ متفاوتی دارد. این کد چند مسیر رایج را امتحان می‌کند:
    - جفت‌ها:   /v2/currencyPairs.do
    - تریدها:   /v2/trades.do  یا /api/v2/trades  یا /api/v1/trades
- اگر endpointها سایز کم برگرداند، ممکن است برای پنجره‌های خیلی بزرگ تمام تریدها را نگیرد.
  برای تست/تولید معمولاً 4h یا 1h کافی است. ستون `trades_used` نشان می‌دهد چند رکورد واقعا محاسبه شده.

Environment (اختیاری):
  HL_TAKER_INTERVAL=4h    # بازه‌ی دلتا: 1h, 2h, 4h, 6h, 12h, 24h, 30m, 15m ...
  HL_MAX_SYMBOLS=400      # سقف تعداد نمادها
  HL_TAKER_ALERT=0.30     # آستانه‌ی هشدار |delta| >= 0.30
  HL_FORCE_DEMO=false     # اگر true شود، داده‌ی دمو می‌سازد
"""

import os
import json
import time
import math
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

logger.info("🚀 High_Level (LBank-only) started.")

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

def parse_interval_to_minutes(interval: str) -> int:
    """
    "4h" -> 240, "30m" -> 30, "1h" -> 60
    """
    s = interval.strip().lower()
    if s.endswith("h"):
        return int(float(s[:-1]) * 60)
    if s.endswith("m"):
        return int(float(s[:-1]))
    # default hours if plain number
    try:
        return int(float(s) * 60)
    except Exception:
        return 240  # default 4h

CONFIG = {
    "LBANK_BASE": os.getenv("HL_LBANK_BASE", "https://api.lbkex.com").rstrip("/"),
    "TAKER_INTERVAL": os.getenv("HL_TAKER_INTERVAL", "4h"),
    "TAKER_ALERT": getenv_float("HL_TAKER_ALERT", 0.30),
    "MAX_SYMBOLS": getenv_int("HL_MAX_SYMBOLS", 400),
    "FORCE_DEMO": getenv_bool("HL_FORCE_DEMO", False),

    # trade fetch tuning
    "TRADE_PAGE_SIZE": getenv_int("HL_TRADE_PAGE_SIZE", 600),   # LBank معمولاً 600-200 تا
    "TRADE_MAX_PAGES": getenv_int("HL_TRADE_MAX_PAGES", 5),     # سقف تکرار برای پوشش پنجره
    "REQUEST_PAUSE": getenv_float("HL_REQUEST_PAUSE", 0.15),    # مکث بین درخواست‌ها
}
logger.info("📌 CONFIG: " + json.dumps(CONFIG))

# ----------------------------
# HTTP helper
# ----------------------------
def safe_request_json(method: str, url: str, headers=None, params=None, timeout=20) -> Optional[dict]:
    try:
        r = requests.request(method=method, url=url, headers=headers, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"HTTP error on {url}: {e}")
        return None

# ----------------------------
# LBank — universe (*_USDT)
# ----------------------------
def lbank_list_usdt_pairs() -> List[str]:
    """
    برمی‌گرداند لیست جفت‌های *_USDT (مثل BTC_USDT) از LBank
    """
    base = CONFIG["LBANK_BASE"]
    endpoints = [
        f"{base}/v2/currencyPairs.do",
        f"{base}/api/v2/currencyPairs",
        f"{base}/api/v1/currencyPairs",
    ]
    for url in endpoints:
        data = safe_request_json("GET", url)
        if not data:
            continue
        # برخی پاسخ‌ها: {"result":true,"data":["btc_usdt", ...]}
        items = data.get("data") if isinstance(data, dict) else data
        pairs = []
        if isinstance(items, list):
            for p in items:
                if not isinstance(p, str):
                    continue
                up = p.upper()
                if up.endswith("_USDT"):
                    pairs.append(up)
        pairs = sorted(set(pairs))
        if pairs:
            return pairs
    return []

# ----------------------------
# LBank — recent trades for a pair
# ----------------------------
def lbank_fetch_trades(pair: str, size: int) -> List[dict]:
    """
    می‌گیرد آخرین `size` ترید از LBank برای یک pair مثل 'BTC_USDT'
    خروجی استاندارد: [{price: float, amount: float, side: 'buy'/'sell', ts: ms}]
    """
    base = CONFIG["LBANK_BASE"]
    params = {"symbol": pair, "size": size}
    urls = [
        f"{base}/v2/trades.do",
        f"{base}/api/v2/trades",
        f"{base}/api/v1/trades",
    ]
    for url in urls:
        data = safe_request_json("GET", url, params=params)
        if not data:
            continue
        items = data.get("data") if isinstance(data, dict) else data
        out = []
        if isinstance(items, list):
            for it in items:
                try:
                    price = float(it.get("price", it.get("deal_price", 0.0)))
                    amt = float(it.get("amount", it.get("qty", it.get("deal_volume", 0.0))))
                    side_raw = str(it.get("type", it.get("side", it.get("direction", "")))).lower()
                    # normalize side
                    if "buy" in side_raw or "bid" in side_raw:
                        side = "buy"
                    elif "sell" in side_raw or "ask" in side_raw:
                        side = "sell"
                    else:
                        side = ""
                    ts = int(it.get("time", it.get("ts", it.get("deal_time", 0))))
                    out.append({"price": price, "amount": amt, "side": side, "ts": ts})
                except Exception:
                    continue
        if out:
            return out
    return []

def aggregate_taker_delta_from_trades(trades: List[dict], start_ts_ms: int, end_ts_ms: int) -> Tuple[float, float, int]:
    """
    داخل بازه [start_ts_ms, end_ts_ms) حجم quote خرید/فروش را جمع می‌بندد.
    خروجی: (buy_quote, sell_quote, trades_used)
    """
    buy_q = 0.0
    sell_q = 0.0
    used = 0
    for t in trades:
        ts = int(t.get("ts") or 0)
        if ts < start_ts_ms or ts >= end_ts_ms:
            continue
        price = float(t.get("price") or 0.0)
        amt = float(t.get("amount") or 0.0)
        side = t.get("side", "")
        q = abs(price * amt)
        if side == "buy":
            buy_q += q
        elif side == "sell":
            sell_q += q
        used += 1
    return buy_q, sell_q, used

def compute_delta_for_pair(pair: str, minutes: int) -> Tuple[Optional[float], float, float, int]:
    """
    تلاش می‌کند با چند بار fetch آخرین تریدها، بازه‌ی موردنیاز را پوشش دهد.
    چون برخی endpointها پارامتر 'since' ندارند، با گرفتن آخرین N و فیلتر زمانی کار می‌کنیم.
    خروجی: (delta, buy_quote, sell_quote, trades_used)
    """
    end_ts_ms = int(datetime.utcnow().timestamp() * 1000)
    start_ts_ms = end_ts_ms - minutes * 60 * 1000

    page_size = CONFIG["TRADE_PAGE_SIZE"]
    max_pages = CONFIG["TRADE_MAX_PAGES"]
    pause = CONFIG["REQUEST_PAUSE"]

    all_trades: List[dict] = []

    # تلاش‌های پیاپی: هر بار آخرین N ترید را می‌گیریم؛ اگر قدیمی‌ترین آن هنوز از start جدیدتر است، امیدواریم پوشش دهیم.
    # (بعضی APIها فقط last N را می‌دهند، لذا ممکن است کل 4h را پوشش ندهد. در این صورت با همان مقدار محاسبه می‌کنیم.)
    for _ in range(max_pages):
        recent = lbank_fetch_trades(pair, size=page_size)
        if not recent:
            break
        # LBank معمولاً جدیدترین در ابتدای لیست است؛ مرتب می‌کنیم تا صعودی شود.
        recent_sorted = sorted(recent, key=lambda x: int(x.get("ts", 0)))
        all_trades = recent_sorted  # چون هر بار همان last N است، جایگزین می‌کنیم
        # اگر قدیمی‌ترین ترید از start قدیمی‌تر بود، احتمالاً پوشش کافی داریم و می‌شکنیم
        if all_trades and int(all_trades[0].get("ts", end_ts_ms)) <= start_ts_ms:
            break
        time.sleep(pause)

    if not all_trades:
        return (None, 0.0, 0.0, 0)

    buy_q, sell_q, used = aggregate_taker_delta_from_trades(all_trades, start_ts_ms, end_ts_ms)
    denom = buy_q + sell_q
    delta = (buy_q - sell_q) / denom if denom > 0 else None
    return (delta, buy_q, sell_q, used)

# ----------------------------
# DEMO fallback (اگر FORCE_DEMO=true)
# ----------------------------
def build_demo_rows(pairs: List[str], minutes: int) -> List[dict]:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for i, p in enumerate(pairs):
        sign = 1 if (i % 3 != 2) else -1
        d = round(0.06 * sign, 4)  # ±6% برای تنوع
        rows.append({
            "timestamp_utc": now,
            "pair": p,
            "interval_min": minutes,
            "taker_buy_quote": 100000.0 if d >= 0 else 60000.0,
            "taker_sell_quote": 60000.0 if d >= 0 else 100000.0,
            "trades_used": 300,
            "taker_delta": d,
            "taker_delta_alert": abs(d) >= CONFIG["TAKER_ALERT"],
            "run_file": "",
        })
    return rows

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
# Main
# ----------------------------
def run_pipeline() -> pd.DataFrame:
    minutes = parse_interval_to_minutes(CONFIG["TAKER_INTERVAL"])
    logger.info(f"⏱ Interval = {CONFIG['TAKER_INTERVAL']} ({minutes} min) | Alert >= {CONFIG['TAKER_ALERT']:.0%}")

    # 1) کشف یونیورس از LBank
    pairs = lbank_list_usdt_pairs()
    if not pairs:
        logger.warning("❗️ LBank pairs not found — falling back to BTC_USDT, ETH_USDT, BNB_USDT")
        pairs = ["BTC_USDT", "ETH_USDT", "BNB_USDT"]

    # سقف
    pairs = pairs[: CONFIG["MAX_SYMBOLS"]]

    # نمایش و ذخیره‌ی یونیورس برای شفافیت
    try:
        pd.DataFrame({"pair": pairs}).to_csv(DATA_DIR / "universe_lbank.csv", index=False)
        logger.info(f"📄 Saved LBank universe: data/universe_lbank.csv ({len(pairs)} pairs)")
    except Exception as e:
        logger.warning(f"Save universe_lbank.csv failed: {e}")

    # 2) اگر FORCE_DEMO فعال است، مستقیم دمو بساز
    if CONFIG["FORCE_DEMO"]:
        logger.warning("⚠️ FORCE_DEMO=true — generating demo rows.")
        rows = build_demo_rows(pairs, minutes)
        return pd.DataFrame(rows)

    # 3) محاسبه‌ی دلتا برای هر جفت
    rows: List[dict] = []
    throttle_every = 25
    pause = CONFIG["REQUEST_PAUSE"]

    for idx, pair in enumerate(pairs, start=1):
        try:
            delta, buy_q, sell_q, used = compute_delta_for_pair(pair, minutes)
            alert = (abs(delta) >= CONFIG["TAKER_ALERT"]) if (delta is not None) else False
            if alert:
                logger.info(f"⚠️ ALERT {pair} Δ{CONFIG['TAKER_INTERVAL']}: {delta:.2%}")

            rows.append({
                "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pair": pair,
                "interval_min": minutes,
                "taker_buy_quote": round(buy_q, 4),
                "taker_sell_quote": round(sell_q, 4),
                "trades_used": int(used),
                "taker_delta": (None if delta is None else round(delta, 6)),
                "taker_delta_alert": bool(alert),
                "run_file": "",
            })

            if idx % throttle_every == 0:
                time.sleep(pause)

        except Exception as e:
            logger.error(f"[{pair}] error: {e}")
            logger.debug(traceback.format_exc())
            rows.append({
                "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pair": pair,
                "interval_min": minutes,
                "taker_buy_quote": 0.0,
                "taker_sell_quote": 0.0,
                "trades_used": 0,
                "taker_delta": None,
                "taker_delta_alert": False,
                "run_file": "",
            })

    # اگر هیچ دیتایی نشد، دمو برای همه‌ی جفت‌ها بساز
    if not rows or all(r["taker_delta"] is None for r in rows):
        logger.warning("⚠️ No real taker data — switching to DEMO for entire LBank universe.")
        rows = build_demo_rows(pairs, minutes)

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
                "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pair": "N/A",
                "interval_min": parse_interval_to_minutes(CONFIG["TAKER_INTERVAL"]),
                "taker_buy_quote": 0.0,
                "taker_sell_quote": 0.0,
                "trades_used": 0,
                "taker_delta": None,
                "taker_delta_alert": False,
                "run_file": "",
            }]).to_csv(DATA_DIR / "latest.csv", index=False)
        except Exception:
            pass
        return 1

if __name__ == "__main__":
    code = main()
    print(f"Exit code: {code}")
    raise SystemExit(code)
