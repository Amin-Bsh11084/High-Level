# High_Level.py
"""
High_Level — Market-wide taker delta scanner (CoinGlass 4h free-friendly) + Binance fallback
Optional LBank (trades/depth) + Scoring. Saves CSV every run.

Key features:
  - Market universe discovery:
      1) HL_SYMBOLS env (comma-separated bases like "BTC,ETH,SOL")
      2) data/symbols.csv with column 'symbol'
      3) Auto-fetch from Binance USDⓈ-M exchangeInfo (all USDT-margined futures bases)
     Limit via HL_MAX_SYMBOLS (default 200).
  - Taker delta from CoinGlass (interval 4h by default) — no paid upgrade needed.
    Fallback to Binance takerBuySellVol. Final fallback: DEMO (so CSV is never empty).
  - Optional LBank collectors (disabled by default for speed). Enable with HL_ENABLE_LBANK=true.
  - Rotating logs, robust error handling, always writes:
      data/high_level_YYYYMMDD_HHMMSS.csv  and  data/latest.csv

Environment (set in GitHub Actions "Vars/Secrets" or locally):
  # Universe
  HL_SYMBOLS=            # optional (e.g., BTC,ETH,SOL)
  HL_MAX_SYMBOLS=200     # cap the universe size
  # CoinGlass
  COINGLASS_API_KEY=     # optional; if empty we fallback to Binance/demo
  HL_TAKER_INTERVAL=4h   # free-friendly default
  HL_TAKER_ALERT=0.30    # alert when |delta| >= 0.30
  # LBank & Scoring (optional)
  HL_ENABLE_LBANK=false
  HL_TRADE_USD_MIN=100000
  HL_SWEEP_DEPTH_USD=150000
  HL_COINGLASS_LIQ_MIN=200000
  HL_W_LARGE_TRADE=2
  HL_W_SWEEP=2
  HL_W_LIQ=2
  HL_W_OI=1
  HL_W_PCT=1
  HL_LABEL_STRONG=5
  HL_LABEL_PROBABLE=3
  HL_LOOKBACK_MIN=10
  HL_FORCE_DEMO=false
"""

import os
import json
import math
import time
import csv
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
    "SYMBOLS": [s.strip().upper() for s in os.getenv("HL_SYMBOLS", "").split(",") if s.strip()],
    "MAX_SYMBOLS": getenv_int("HL_MAX_SYMBOLS", 200),

    # Free-friendly taker settings
    "COINGLASS_KEY": os.getenv("COINGLASS_API_KEY", "").strip(),
    "COINGLASS_BASE": os.getenv("COINGLASS_BASE", "https://open-api-v4.coinglass.com"),
    "TAKER_INTERVAL": os.getenv("HL_TAKER_INTERVAL", "4h"),
    "TAKER_ALERT": getenv_float("HL_TAKER_ALERT", 0.30),

    # LBank (optional)
    "ENABLE_LBANK": getenv_bool("HL_ENABLE_LBANK", False),
    "LBANK_BASE": os.getenv("HL_LBANK_BASE", "https://api.lbkex.com"),
    "LOOKBACK_MIN": getenv_int("HL_LOOKBACK_MIN", 10),

    # Scoring thresholds/weights
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

# ----------------------------
# Universe discovery
# ----------------------------
def load_universe() -> List[str]:
    """
    Returns list of BASE symbols (e.g., BTC, ETH, SOL, ...).
    Precedence: HL_SYMBOLS env > data/symbols.csv > Binance USDⓈ-M exchangeInfo (USDT-quote).
    Limited to CONFIG['MAX_SYMBOLS'].
    """
    # 1) Explicit env list
    if CONFIG["SYMBOLS"]:
        bases = [s.strip().upper() for s in CONFIG["SYMBOLS"] if s.strip()]
        logger.info(f"Universe from HL_SYMBOLS ({len(bases)}).")
        return bases[: CONFIG["MAX_SYMBOLS"]]

    # 2) Local CSV (data/symbols.csv with column 'symbol')
    csv_path = DATA_DIR / "symbols.csv"
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            if "symbol" in df.columns:
                vals = [str(x).strip().upper() for x in df["symbol"].tolist() if str(x).strip()]
                # Accept either BASE or BASEUSDT; normalize to BASE
                bases = [v[:-4] if v.endswith("USDT") else v for v in vals]
                bases = sorted(set(bases))
                logger.info(f"Universe from data/symbols.csv ({len(bases)}).")
                return bases[: CONFIG["MAX_SYMBOLS"]]
        except Exception as e:
            logger.warning(f"symbols.csv read failed: {e}")

    # 3) Binance USDⓈ-M futures exchangeInfo
    try:
        info = safe_request_json("GET", "https://fapi.binance.com/fapi/v1/exchangeInfo")
        bases = []
        if info and isinstance(info, dict) and "symbols" in info:
            for sym in info["symbols"]:
                # Filter tradable USDT margined futures
                if sym.get("status") == "TRADING" and sym.get("quoteAsset") == "USDT":
                    base = sym.get("baseAsset", "").upper()
                    if base:
                        bases.append(base)
        bases = sorted(set(bases))
        logger.info(f"Universe from Binance futures (USDT): {len(bases)} bases.")
        return bases[: CONFIG["MAX_SYMBOLS"]]
    except Exception as e:
        logger.error(f"Binance exchangeInfo failed: {e}")

    # Fallback tiny set
    logger.warning("Universe fallback to defaults [BTC,ETH,BNB].")
    return ["BTC", "ETH", "BNB"][: CONFIG["MAX_SYMBOLS"]]

# ----------------------------
# Optional LBank (disabled by default)
# ----------------------------
def collect_recent_trades_lbank(symbol: str, lookback_min: int) -> List[dict]:
    if CONFIG["FORCE_DEMO"] or not CONFIG["ENABLE_LBANK"]:
        return []
    base = CONFIG["LBANK_BASE"].rstrip("/")
    pair = f"{symbol}_USDT"
    urls = [f"{base}/v2/trades.do", f"{base}/api/v2/trades", f"{base}/api/v1/trades"]
    params = {"symbol": pair, "size": 200}
    since_ts = int((datetime.utcnow() - timedelta(minutes=lookback_min)).timestamp() * 1000)
    for url in urls:
        data = safe_request_json("GET", url, params=params)
        if not data: 
            continue
        items = data.get("data") if isinstance(data, dict) else data
        out = []
        if isinstance(items, list):
            for it in items:
                try:
                    price = float(it.get("price", 0.0))
                    qty = float(it.get("amount", it.get("qty", 0.0)))
                    side = str(it.get("type", it.get("side", ""))).lower()
                    ts = int(it.get("time", it.get("ts", 0)))
                    if ts and ts < since_ts: 
                        continue
                    out.append({"price": price, "qty": qty, "side": side, "ts": ts})
                except Exception:
                    continue
        if out:
            return out
    return []

def collect_orderbook_lbank(symbol: str) -> dict:
    if CONFIG["FORCE_DEMO"] or not CONFIG["ENABLE_LBANK"]:
        return {}
    base = CONFIG["LBANK_BASE"].rstrip("/")
    pair = f"{symbol}_USDT"
    urls = [f"{base}/v2/depth.do", f"{base}/api/v2/depth", f"{base}/api/v1/depth"]
    params = {"symbol": pair, "size": 60}
    for url in urls:
        data = safe_request_json("GET", url, params=params)
        if not data:
            continue
        d = data.get("data") if isinstance(data, dict) else data
        if isinstance(d, dict):
            def _norm(x):
                out = []
                for i in x:
                    if isinstance(i, list) and len(i) >= 2:
                        out.append([float(i[0]), float(i[1])])
                    elif isinstance(i, dict):
                        out.append([float(i.get("price", 0)), float(i.get("amount", 0))])
                return out
            return {"bids": _norm(d.get("bids", [])), "asks": _norm(d.get("asks", []))}
    return {}

# ----------------------------
# Scoring helpers (optional)
# ----------------------------
def detect_large_trades(trades: List[dict], price_hint: Optional[float], min_usd: float) -> Tuple[bool, float, str]:
    found = False; max_usd = 0.0; max_side = ""
    p = price_hint or (trades[0]["price"] if trades else 0.0)
    for t in trades:
        price = t["price"] or p
        usd = abs(price * t["qty"])
        side = (t.get("side") or "").lower()
        if usd > max_usd:
            max_usd, max_side = usd, side
        if usd >= min_usd:
            found = True
    return found, max_usd, max_side

def detect_orderbook_sweep(orderbook: dict, sweep_depth_usd: float, price_hint: Optional[float]) -> Tuple[bool, float]:
    if not orderbook:
        return (False, 0.0)
    bids = orderbook.get("bids", [])[:15]; asks = orderbook.get("asks", [])[:15]
    bid_usd = sum(p * q for p, q in bids); ask_usd = sum(p * q for p, q in asks)
    swept = (bid_usd <= sweep_depth_usd) or (ask_usd <= sweep_depth_usd)
    return (swept, min(bid_usd, ask_usd))

def coinglass_signal(m: dict, liq_min: float) -> Tuple[bool, float, float, float, float]:
    if not m:
        return (False, 0.0, 0.0, 0.0, 0.0)
    liq_buy = float(m.get("liq_buy_usd", 0.0))
    liq_sell = float(m.get("liq_sell_usd", 0.0))
    has_spike = (liq_buy >= liq_min) or (liq_sell >= liq_min)
    oi_change = float(m.get("oi_change_pct", 0.0))
    funding = float(m.get("funding_rate", 0.0))
    return (has_spike, liq_buy, liq_sell, oi_change, funding)

def fuse_score(lt, sw, cg_sig, pct_change: float) -> Tuple[float, str, str]:
    (lt_found, lt_max_usd, lt_side) = lt
    (sw_found, _) = sw
    (liq_spike, liq_buy, liq_sell, oi_change, _) = cg_sig
    score = 0.0
    if lt_found: score += CONFIG["W_LARGE_TRADE"]
    if sw_found: score += CONFIG["W_SWEEP"]
    if liq_spike: score += CONFIG["W_LIQ"]
    if abs(oi_change) > 0: score += CONFIG["W_OI"] * (1.0 if oi_change > 0 else 0.5)
    if abs(pct_change) > 0: score += CONFIG["W_PCT"] * (1.0 if pct_change > 0 else 0.5)

    if score >= CONFIG["LABEL_STRONG"]: label = "strong"
    elif score >= CONFIG["LABEL_PROBABLE"]: label = "probable"
    elif score > 0: label = "weak"
    else: label = "neutral"

    if lt_side in ("buy","sell"): side = lt_side
    elif liq_spike: side = "buy" if liq_buy >= liq_sell else "sell"
    else: side = ""
    return score, label, side

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

    # CoinGlass first
    try:
        cg = fetch_coinglass_taker_interval(pair, interval=interval)
        if cg:
            buy, sell = cg["buyVol"], cg["sellVol"]
            denom = buy + sell
            return 0.0 if denom <= 0 else (buy - sell) / denom
    except Exception as e:
        logger.debug(f"CoinGlass delta error {base_symbol}: {e}")

    # Binance fallback (public)
    try:
        url = "https://fapi.binance.com/futures/data/takerBuySellVol"
        params = {"symbol": pair, "period": interval, "limit": 1}
        data = safe_request_json("GET", url, params=params, timeout=15)
        if data and isinstance(data, list):
            last = data[-1]
            buy = float(last.get("buyVol", 0.0))
            sell = float(last.get("sellVol", 0.0))
            denom = buy + sell
            return 0.0 if denom <= 0 else (buy - sell) / denom
    except Exception as e:
        logger.debug(f"Binance fallback delta error {base_symbol}: {e}")

    return None

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
# Demo rows (fallback)
# ----------------------------
def build_demo_rows(symbols: List[str]) -> List[dict]:
    rows = []
    now = now_utc_iso()
    for i, s in enumerate(symbols[:10] or ["BTC","ETH","BNB"]):
        delta = (0.35 if s=="BTC" else (-0.1 if s=="ETH" else 0.05))
        rows.append({
            "timestamp_utc": now,
            "symbol": s,
            "price_last": 0.0,
            "pct_change": 0.0,
            "total_quote_5m": None,
            "max_trade_usd": 0.0,
            "whale_detected": abs(delta) >= 0.05,
            "whale_side": "buy" if delta>0 else ("sell" if delta<0 else ""),
            "orderbook_sweep": False,
            "sweep_depth_usd": 0.0,
            "coinglass_liq_buy_usd": 0.0,
            "coinglass_liq_sell_usd": 0.0,
            "oi_change_pct": 0.0,
            "funding_rate": 0.0,
            "signal_score": 0.0,
            "signal_label": "neutral",
            "taker_delta_interval": delta,
            "taker_delta_alert_interval": abs(delta) >= CONFIG["TAKER_ALERT"],
            "run_file": "",
        })
    return rows

# ----------------------------
# Main pipeline
# ----------------------------
def run_pipeline() -> pd.DataFrame:
    bases = load_universe()
    logger.info(f"🧪 Universe size = {len(bases)} (cap={CONFIG['MAX_SYMBOLS']}) | Interval={CONFIG['TAKER_INTERVAL']}")

    rows: List[dict] = []
    lookback = CONFIG["LOOKBACK_MIN"]

    # For large universes, avoid hammering APIs too fast
    throttle_every = 25
    pause_sec = 0.7  # gentle

    for idx, sym in enumerate(bases, start=1):
        try:
            # Optional LBank collectors (disabled by default)
            trades = collect_recent_trades_lbank(sym, lookback) if CONFIG["ENABLE_LBANK"] else []
            orderbook = collect_orderbook_lbank(sym) if CONFIG["ENABLE_LBANK"] else {}

            price_hint = None
            if trades:
                price_hint = float(trades[-1]["price"])
            elif orderbook.get("bids") or orderbook.get("asks"):
                b0 = orderbook.get("bids", [[0,0]])[0][0] if orderbook.get("bids") else 0.0
                a0 = orderbook.get("asks", [[0,0]])[0][0] if orderbook.get("asks") else 0.0
                price_hint = (b0 + a0)/2.0 if (b0 and a0) else (b0 or a0 or None)

            lt = detect_large_trades(trades, price_hint, CONFIG["TRADE_USD_MIN"]) if CONFIG["ENABLE_LBANK"] else (False, 0.0, "")
            sw = detect_orderbook_sweep(orderbook, CONFIG["SWEEP_DEPTH_USD"], price_hint) if CONFIG["ENABLE_LBANK"] else (False, 0.0)
            cg_sig = coinglass_signal({}, CONFIG["COINGLASS_LIQ_MIN"])  # stub metrics (not used here)
            pct_change = 0.0
            score, label, side = fuse_score(lt, sw, cg_sig, pct_change)

            # taker delta via CoinGlass 4h (free) with Binance fallback
            delta = compute_taker_delta_with_fallback(sym, interval=CONFIG["TAKER_INTERVAL"])
            alert = (abs(delta) >= CONFIG["TAKER_ALERT"]) if (delta is not None) else False
            if alert:
                logger.info(f"⚠️ ALERT {sym} Δ{CONFIG['TAKER_INTERVAL']}: {delta:.2%}")

            row = {
                "timestamp_utc": now_utc_iso(),
                "symbol": sym,
                "price_last": float(price_hint or 0.0),
                "pct_change": pct_change,
                "total_quote_5m": None,
                "max_trade_usd": float(lt[1]) if lt else 0.0,
                "whale_detected": (score > 0) or alert,
                "whale_side": side if side else ("buy" if (delta or 0) > 0 else ("sell" if (delta or 0) < 0 else "")),
                "orderbook_sweep": bool(sw[0]),
                "sweep_depth_usd": float(sw[1]),
                "coinglass_liq_buy_usd": 0.0,
                "coinglass_liq_sell_usd": 0.0,
                "oi_change_pct": 0.0,
                "funding_rate": 0.0,
                "signal_score": float(score),
                "signal_label": str(label),
                "taker_delta_interval": delta,
                "taker_delta_alert_interval": alert,
                "run_file": "",
            }
            rows.append(row)

            # Throttle
            if idx % throttle_every == 0:
                time.sleep(pause_sec)

        except Exception as e:
            logger.error(f"[{sym}] pipeline error: {e}")
            logger.debug(traceback.format_exc())

    # DEMO fallback if everything empty or None
    if not rows or all(r["taker_delta_interval"] is None for r in rows):
        logger.warning("⚠️ No real taker data — switching to DEMO rows.")
        rows = build_demo_rows(bases)

    return pd.DataFrame(rows)

def main() -> int:
    try:
        df = run_pipeline()
        out_path = save_output_csv(df)
        df["run_file"] = out_path
        df.to_csv(DATA_DIR / "latest.csv", index=False)
        logger.info(f"✅ Completed. Rows: {len(df)}")
        logger.info("\n" + df.head(min(15, len(df))).to_string(index=False))
        return 0
    except Exception as e:
        logger.error(f"❌ Fatal: {e}")
        logger.debug(traceback.format_exc())
        try:
            df = pd.DataFrame([{
                "timestamp_utc": now_utc_iso(),
                "symbol": "N/A",
                "price_last": 0.0,
                "pct_change": 0.0,
                "total_quote_5m": None,
                "max_trade_usd": 0.0,
                "whale_detected": False,
                "whale_side": "",
                "orderbook_sweep": False,
                "sweep_depth_usd": 0.0,
                "coinglass_liq_buy_usd": 0.0,
                "coinglass_liq_sell_usd": 0.0,
                "oi_change_pct": 0.0,
                "funding_rate": 0.0,
                "signal_score": 0.0,
                "signal_label": "neutral",
                "taker_delta_interval": None,
                "taker_delta_alert_interval": False,
                "run_file": "",
            }])
            save_output_csv(df)
        except Exception:
            pass
        return 1

if __name__ == "__main__":
    code = main()
    print(f"Exit code: {code}")
    raise SystemExit(code)
