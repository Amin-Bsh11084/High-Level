# High_Level.py — LBank 4h Movers (±30%)
"""
چه می‌کند؟
- از LBank تمام جفت‌های *_USDT را می‌گیرد.
- با استفاده از معاملات اخیر LBank، برای هر جفت قیمت "آغازین" و "پایانی" ۴ ساعت اخیر را محاسبه می‌کند:
    price_first = قیمت اولین ترید داخل پنجره 4h
    price_last  = قیمت آخرین ترید داخل پنجره 4h
    pct_change  = (price_last - price_first) / price_first
- فقط جفت‌هایی را در CSV خروجی می‌نویسد که |pct_change| >= ALERT (پیش‌فرض 0.30 معادل 30%).
- خروجی مینیمال و تمیز: pair, price_first, price_last, pct_change_4h, trades_used, alert

ENV اختیاری:
  HL_MAX_SYMBOLS=400      # سقف تعداد جفت‌ها
  HL_PRICE_ALERT=0.30     # آستانه‌ی 30% (|Δ|>=0.30)
  HL_FORCE_DEMO=false     # اگر True باشد، داده‌ی دمو می‌سازد (برای تست آرتیفکت)
"""

import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import requests
import pandas as pd

# ----------------------------
# تنظیمات
# ----------------------------
def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "")
    if not v: return default
    return v.strip().lower() in ("1","true","yes","on","y")

def _get_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except: return default

def _get_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v else default
    except: return default

CFG = {
    "LBANK_BASE": "https://api.lbkex.com",
    "MAX_SYMBOLS": _get_int("HL_MAX_SYMBOLS", 400),
    "ALERT": _get_float("HL_PRICE_ALERT", 0.30),  # 30%
    "FORCE_DEMO": _get_bool("HL_FORCE_DEMO", False),
    "REQ_TIMEOUT": 15,
    "REQ_PAUSE": 0.12,     # مکث کوتاه بین جفت‌ها
    "TRADE_SIZE": 800,     # تلاش برای پوشش 4h با آخرین N معامله
}

DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = Path("logs/run_log.txt"); LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass

# ----------------------------
# HTTP ساده
# ----------------------------
def _get_json(url: str, params=None) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=CFG["REQ_TIMEOUT"])
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"HTTP error: {url} :: {e}")
        return None

# ----------------------------
# LBank: یونیورس و معاملات
# ----------------------------
def list_lbank_usdt_pairs() -> List[str]:
    base = CFG["LBANK_BASE"].rstrip("/")
    for path in ("/v2/currencyPairs.do", "/api/v2/currencyPairs", "/api/v1/currencyPairs"):
        data = _get_json(base + path)
        if not data: 
            continue
        items = data.get("data") if isinstance(data, dict) else data
        if isinstance(items, list):
            pairs = sorted({str(p).upper() for p in items if str(p).upper().endswith("_USDT")})
            if pairs:
                return pairs
    return []

def fetch_lbank_trades(pair: str, size: int) -> List[dict]:
    """
    خروجی استاندارد: [{price: float, ts: ms}]
    (سمت خرید/فروش مهم نیست چون فقط تغییر قیمت می‌خواهیم)
    """
    base = CFG["LBANK_BASE"].rstrip("/")
    params = {"symbol": pair, "size": size}
    for path in ("/v2/trades.do", "/api/v2/trades", "/api/v1/trades"):
        data = _get_json(base + path, params=params)
        if not data: 
            continue
        items = data.get("data") if isinstance(data, dict) else data
        out = []
        if isinstance(items, list):
            for it in items:
                try:
                    price = float(it.get("price", it.get("deal_price", 0.0)))
                    ts    = int(it.get("time", it.get("ts", it.get("deal_time", 0))))
                    out.append({"price":price, "ts":ts})
                except: 
                    continue
        if out:
            # صعودی بر اساس زمان
            return sorted(out, key=lambda x: int(x.get("ts", 0)))
    return []

# ----------------------------
# محاسبه تغییر قیمت 4h
# ----------------------------
def price_change_4h_from_trades(trades: List[dict], end_ms: int, window_min: int = 240) -> Tuple[Optional[float], Optional[float], Optional[float], int]:
    """
    از لیست معاملات مرتب‌شده (صعودی) قیمت اولین و آخرین معامله در پنجره 4h را برمی‌گرداند.
    خروجی: (pct_change, price_first, price_last, trades_used)
    """
    start_ms = end_ms - window_min * 60 * 1000
    in_win = [t for t in trades if start_ms <= int(t.get("ts") or 0) < end_ms]
    if not in_win:
        return (None, None, None, 0)
    price_first = float(in_win[0]["price"])
    price_last  = float(in_win[-1]["price"])
    if price_first <= 0:
        return (None, price_first, price_last, len(in_win))
    pct = (price_last - price_first) / price_first
    return (pct, price_first, price_last, len(in_win))

# ----------------------------
# DEMO (برای مواقعی که دیتا نیست/تست)
# ----------------------------
def build_demo(pairs: List[str]) -> pd.DataFrame:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for i, p in enumerate(pairs):
        # برای تست، چندتا را بالای 30% بگذاریم
        pct = 0.35 if (i % 7 == 0) else (-0.32 if (i % 11 == 0) else 0.08)
        rows.append({
            "timestamp_utc": now,
            "pair": p,
            "price_first": 1.0,
            "price_last": round(1.0 * (1+pct), 6),
            "pct_change_4h": round(pct, 6),
            "trades_used": 200,
            "alert": abs(pct) >= CFG["ALERT"],
        })
    # فقط آلارمی‌ها را نگه می‌داریم
    df = pd.DataFrame(rows)
    return df[df["alert"]].reset_index(drop=True)

# ----------------------------
# ذخیره CSV
# ----------------------------
def save_csv(df: pd.DataFrame) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = DATA_DIR / f"high_level_{ts}.csv"
    # فقط ستون‌های هدف
    cols = ["timestamp_utc","pair","price_first","price_last","pct_change_4h","trades_used","alert"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_csv(out, index=False)
    df.to_csv(DATA_DIR / "latest.csv", index=False)
    log(f"CSV saved -> {out}")
    return str(out)

# ----------------------------
# اجرای اصلی
# ----------------------------
def main() -> int:
    try:
        log(f"Target window = 4h | Threshold = {CFG['ALERT']:.0%}")

        # یونیورس
        pairs = list_lbank_usdt_pairs()
        if not pairs:
            log("No pairs from LBank; fallback to 3.")
            pairs = ["BTC_USDT", "ETH_USDT", "BNB_USDT"]
        pairs = pairs[: CFG["MAX_SYMBOLS"]]
        pd.DataFrame({"pair": pairs}).to_csv(DATA_DIR / "universe_lbank.csv", index=False)

        if CFG["FORCE_DEMO"]:
            df = build_demo(pairs)
            save_csv(df)
            return 0

        end_ms = int(datetime.utcnow().timestamp() * 1000)
        rows = []
        # جمع‌آوری
        hit_any = False
        for i, pair in enumerate(pairs, start=1):
            trades = fetch_lbank_trades(pair, size=CFG["TRADE_SIZE"])
            pct, p_first, p_last, used = price_change_4h_from_trades(trades, end_ms=end_ms, window_min=240)
            alert = (abs(pct) >= CFG["ALERT"]) if (pct is not None) else False
            if alert:
                hit_any = True
                rows.append({
                    "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "pair": pair,
                    "price_first": None if p_first is None else round(p_first, 8),
                    "price_last": None if p_last  is None else round(p_last, 8),
                    "pct_change_4h": None if pct    is None else round(pct, 6),
                    "trades_used": used,
                    "alert": True,
                })
            if i % 30 == 0:
                time.sleep(CFG["REQ_PAUSE"])

        # اگر هیچ جفتی به آستانه نرسید، CSV فقط با هدر و بدون سطر ذخیره می‌شود (قابل‌خواندن است)
        df = pd.DataFrame(rows)
        if df.empty:
            log("No 4h movers beyond threshold; writing empty (header-only) CSV.")
            df = pd.DataFrame(columns=["timestamp_utc","pair","price_first","price_last","pct_change_4h","trades_used","alert"])

        save_csv(df)
        log(f"Done. Movers count = {len(df)}")
        return 0

    except Exception as e:
        log(f"Fatal error: {e}")
        try:
            pd.DataFrame(columns=["timestamp_utc","pair","price_first","price_last","pct_change_4h","trades_used","alert"])\
              .to_csv(DATA_DIR / "latest.csv", index=False)
        except:
            pass
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
