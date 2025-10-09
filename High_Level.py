# High_Level.py — LBank 4h Movers via KLINE (±10%)
"""
چه می‌کند؟
- از LBank لیست تمام جفت‌ها را می‌گیرد و فقط *_USDT نگه می‌دارد (بدون توکن‌های اهرمی مثل 3L/3S/5L/5S).
- برای هر جفت با /v2/kline.do آخرین دو کندل 4h را می‌گیرد (type=hour4, size=2) و پس از مرتب‌سازی زمانی،
  «آخرین کندل کامل» را گزارش می‌کند.
- روی «آخرین کندل کامل» درصد تغییر را محاسبه می‌کند: (close - open) / open.
- فقط جفت‌هایی که |pct_change| >= ALERT (پیش‌فرض 0.10) باشند وارد CSV می‌شوند.
- خروجی تمیز: timestamp_utc, pair, open_4h, close_4h, pct_change_4h, kline_ts, alert

ENVهای اختیاری:
  HL_MAX_SYMBOLS=400
  HL_PRICE_ALERT=0.10
  HL_FORCE_DEMO=false
  HL_REQ_TIMEOUT=15
  HL_REQ_PAUSE=0.05
"""
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests
import pandas as pd

# ----------------------------
# Config & helpers
# ----------------------------
def _b(name: str, default: bool) -> bool:
    v = os.getenv(name, "")
    return default if not v else v.strip().lower() in ("1","true","yes","on","y")

def _i(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "");  return int(v) if v else default
    except: return default

def _f(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "");  return float(v) if v else default
    except: return default

CFG = {
    "BASE": "https://api.lbkex.com",
    "MAX_SYMBOLS": _i("HL_MAX_SYMBOLS", 400),
    "ALERT": _f("HL_PRICE_ALERT", 0.10),   # ← پیش‌فرض 10%
    "FORCE_DEMO": _b("HL_FORCE_DEMO", False),
    "REQ_TIMEOUT": _i("HL_REQ_TIMEOUT", 15),
    "REQ_PAUSE": _f("HL_REQ_PAUSE", 0.05),  # بین درخواست‌ها
}

DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = Path("logs/run_log.txt"); LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass

# ----------------------------
# HTTP (Session + retry-lite)
# ----------------------------
_SESSION: Optional[requests.Session] = None

def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({"User-Agent":"HighLevelBot/1.0"})
        _SESSION = s
    return _SESSION

def _get_json(path: str, params=None, tries: int = 3) -> Optional[dict | list]:
    url = CFG["BASE"].rstrip("/") + path
    for k in range(tries):
        try:
            r = _session().get(url, params=params, timeout=CFG["REQ_TIMEOUT"])
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log(f"HTTP error [{k+1}/{tries}]: {url} :: {e}")
            time.sleep(0.2 * (k+1))
    return None

# ----------------------------
# LBank endpoints (official)
# - Available trading pairs: GET /v2/currencyPairs.do
# - Kline (candles):         GET /v2/kline.do  (type=hour4, size up to 2000)
# ----------------------------
_LEVERAGED_RE = re.compile(r".*_(\d+(l|s))_usdt$")  # btc3l_usdt, eth5s_usdt, ...

def _is_spot_usdt(symbol: str) -> bool:
    s = symbol.lower()
    if not s.endswith("_usdt"):
        return False
    # حذف ETF/Leveraged (3L/3S/5L/5S و ...)
    if _LEVERAGED_RE.match(s):
        return False
    return True

def list_usdt_pairs() -> List[str]:
    data = _get_json("/v2/currencyPairs.do")
    items = data if isinstance(data, list) else (data or {}).get("data", [])
    pairs = [str(p).lower() for p in items if _is_spot_usdt(str(p))]
    pairs = sorted(set(pairs))
    return pairs

def fetch_last_two_4h_candles(symbol: str) -> Optional[Tuple[int,float,float]]:
    """
    برمی‌گرداند: (kline_ts_sec, open, close) برای «آخرین کندل کامل».
    پیاده‌سازی امن: آرایه‌ی دریافتی را بر اساس timestamp مرتب می‌کنیم و کندل قبل از آخر را برمی‌داریم.
    """
    now_sec = int(datetime.now(timezone.utc).timestamp())
    params = {
        "symbol": symbol,
        "size": 2,
        "type": "hour4",
        "time": now_sec
    }
    data = _get_json("/v2/kline.do", params=params)
    if not isinstance(data, list) or len(data) < 2:
        return None
    try:
        data_sorted = sorted(data, key=lambda x: int(x[0]))  # [ts, o, h, l, c, v]
        prev = data_sorted[-2]  # آخرین کندل کامل
        ts_sec = int(prev[0])
        o = float(prev[1]); c = float(prev[4])
        return (ts_sec, o, c)
    except Exception:
        return None

# ----------------------------
# DEMO (اختیاری برای تست آرتیفکت)
# ----------------------------
def build_demo(pairs: List[str]) -> pd.DataFrame:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for i, p in enumerate(pairs):
        pct = 0.12 if (i % 7 == 0) else (-0.11 if (i % 11 == 0) else 0.04)
        rows.append({
            "timestamp_utc": now,
            "pair": p.upper(),
            "open_4h": 1.0,
            "close_4h": round(1.0*(1+pct), 8),
            "pct_change_4h": round(pct, 6),
            "kline_ts": int(datetime.now(timezone.utc).timestamp()) - 4*3600,
            "alert": abs(pct) >= CFG["ALERT"]
        })
    df = pd.DataFrame(rows)
    return df[df["alert"]].reset_index(drop=True)

# ----------------------------
# Save CSV
# ----------------------------
def save_csv(df: pd.DataFrame) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = DATA_DIR / f"high_level_{ts}.csv"
    cols = ["timestamp_utc","pair","open_4h","close_4h","pct_change_4h","kline_ts","alert"]
    for c in cols:
        if c not in df.columns: df[c] = None
    df = df[cols]
    df.to_csv(out, index=False)
    df.to_csv(DATA_DIR / "latest.csv", index=False)
    log(f"CSV saved -> {out}")
    return str(out)

# ----------------------------
# Main
# ----------------------------
def main() -> int:
    try:
        log(f"Window = 4h (KLINE) | Threshold = {CFG['ALERT']:.0%}")

        pairs = list_usdt_pairs()
        if not pairs:
            log("⚠️ LBank pairs list empty → fallback to core trio.")
            pairs = ["btc_usdt","eth_usdt","bnb_usdt"]
        pairs = pairs[:CFG["MAX_SYMBOLS"]]
        pd.DataFrame({"pair": [p.upper() for p in pairs]}).to_csv(DATA_DIR / "universe_lbank.csv", index=False)

        if CFG["FORCE_DEMO"]:
            df = build_demo(pairs)
            save_csv(df)
            return 0

        rows = []
        count = 0
        for p in pairs:
            res = fetch_last_two_4h_candles(p)
            if res is None:
                continue
            ts_sec, o, c = res
            if o <= 0:
                continue
            pct = (c - o) / o
            if abs(pct) >= CFG["ALERT"]:
                rows.append({
                    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "pair": p.upper(),
                    "open_4h": round(o, 8),
                    "close_4h": round(c, 8),
                    "pct_change_4h": round(pct, 6),
                    "kline_ts": ts_sec,
                    "alert": True
                })
            count += 1
            # 200 req / 10s → مکث کوتاه برای احتیاط
            if count % 50 == 0:
                time.sleep(2.5)
            else:
                time.sleep(CFG["REQ_PAUSE"])

        df = pd.DataFrame(rows)
        if df.empty:
            log("No 4h movers beyond threshold; writing header-only CSV (no alerts).")
            df = pd.DataFrame(columns=["timestamp_utc","pair","open_4h","close_4h","pct_change_4h","kline_ts","alert"])

        save_csv(df)
        log(f"Done. Alerts = {len(df)}")
        return 0

    except Exception as e:
        log(f"Fatal error: {e}")
        try:
            pd.DataFrame(columns=["timestamp_utc","pair","open_4h","close_4h","pct_change_4h","kline_ts","alert"])\
              .to_csv(DATA_DIR / "latest.csv", index=False)
        except:
            pass
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
