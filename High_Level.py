# High_Level.py
"""
High_Level — LBank whale monitor (Trades + Orderbook Sweep + Coinglass + Scoring)

Pipeline:
  1) Collector: recent trades (LBank), orderbook snapshot (LBank), futures metrics (Coinglass optional)
  2) Analyzer: detect_large_trades, detect_orderbook_sweep, coinglass_signal
  3) Taker Delta (interval 4h by default) from CoinGlass; fallback to Binance; fallback to DEMO
  4) Score & Label
  5) Save CSV -> data/high_level_YYYYMMDD_HHMMSS.csv AND data/latest.csv

Config (env):
  HL_SYMBOLS=BTC,ETH,BNB
  COINGLASS_API_KEY=<your key>    # optional; if missing we use fallback/demo
  HL_TAKER_INTERVAL=4h            # default 4h for free plans
  HL_TAKER_ALERT=0.30             # alert threshold on |delta| (0..1), default 0.30
  HL_TRADE_USD_MIN=100000
  HL_SWEEP_DEPTH_USD=150000
  HL_COINGLASS_LIQ_MIN=200000
  HL_W_LARGE_TRADE=2 HL_W_SWEEP=2 HL_W_LIQ=2 HL_W_OI=1 HL_W_PCT=1
  HL_LABEL_STRONG=5 HL_LABEL_PROBABLE=3
  HL_LOOKBACK_MIN=10
  HL_FORCE_DEMO=false
"""

import os
import json
import math
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd

# ----------------------------
# Logging (prefers loguru)
# ----------------------------
_HAS_LOGURU = False
try:
    from loguru import logger  # type: ignore
    _HAS_LOGURU = True
except Exception:
    import logging
    logger = logging.getLogger("High_Level")

# --- paths & state ---
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "run_log.txt"

STATE_DIR = Path(".state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

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
    stream_h = logging.StreamHandler()
    stream_h.setFormatter(fmt)
    file_h = RotatingFileHandler(str(LOG_FILE), maxBytes=1_000_000, backupCount=5)
    file_h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(stream_h)
        logger.addHandler(file_h)

logger.info("🚀 High_Level started (logging configured).")

# ----------------------------
# CONFIG (via env)
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
    "SYMBOLS": [s.strip().upper() for s in os.getenv("HL_SYMBOLS", "BTC,ETH,BNB").split(",") if s.strip()],
    "LBANK_BASE": os.getenv("HL_LBANK_BASE", "https://api.lbkex.com"),
    "COINGLASS_KEY": os.getenv("COINGLASS_API_KEY", "").strip(),
    "COINGLASS_BASE": os.getenv("COINGLASS_BASE", "https://open-api-v4.coinglass.com"),
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
    "LOOKBACK_MIN": getenv_int("HL_LOOKBACK_MIN", 10),
    "FORCE_DEMO": getenv_bool("HL_FORCE_DEMO", False),
    # NEW: free-friendly taker settings
    "TAKER_INTERVAL": os.getenv("HL_TAKER_INTERVAL", "4h"),
    "TAKER_ALERT": getenv_float("HL_TAKER_ALERT", 0.30),
}
logger.info(
    "📌 CONFIG: " +
    json.dumps({k: (v if k != "COINGLASS_KEY" else ("***" if v else "")) for k, v in CONFIG.items()}, ensure_ascii=False)
)

# ----------------------------
# Helpers
# ----------------------------
def now_utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def safe_request_json(method: str, url: str, headers=None, params=None, timeout=15) -> Optional[dict]:
    try:
        r = requests.request(method=method, url=url, headers=headers, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"HTTP error on {url}: {e}")
        return None

# ----------------------------
# Collector (LBank — best effort, optional)
# ----------------------------
def collect_recent_trades_lbank(symbol: str, lookback_min: int) -> List[dict]:
    if CONFIG["FORCE_DEMO"]:
        return []
    base = CONFIG["LBANK_BASE"].rstrip("/")
    pair = f"{symbol}_USDT"
    url_candidates = [
        f"{base}/v2/trades.do",
        f"{base}/api/v2/trades",
        f"{base}/api/v1/trades",
    ]
    params = {"symbol": pair, "size": 200}
    since_ts_ms = int((datetime.utcnow() - timedelta(minutes=lookback_min)).timestamp() * 1000)

    for url in url_candidates:
        data = safe_request_json("GET", url, params=params)
        if not data:
            continue
        trades = []
        items = data.get("data") if isinstance(data, dict) else data
        if isinstance(items, list):
            for it in items:
                price = float(it.get("price", 0.0))
                qty = float(it.get("amount", it.get("qty", 0.0)))
                side = str(it.get("type", it.get("side", ""))).lower()
                ts = int(it.get("time", it.get("ts", 0)))
                if ts and ts < since_ts_ms:
                    continue
                trades.append({"price": price, "qty": qty, "side": side, "ts": ts})
        if trades:
            logger.info(f"✔️ LBank trades fetched for {symbol}: {len(trades)}")
            return trades

    logger.warning(f"No trades fetched for {symbol} (LBank).")
    return []

def collect_orderbook_lbank(symbol: str) -> dict:
    if CONFIG["FORCE_DEMO"]:
        return {}
    base = CONFIG["LBANK_BASE"].rstrip("/")
    pair = f"{symbol}_USDT"
    url_candidates = [
        f"{base}/v2/depth.do",
        f"{base}/api/v2/depth",
        f"{base}/api/v1/depth",
    ]
    params = {"symbol": pair, "size": 60}

    for url in url_candidates:
        data = safe_request_json("GET", url, params=params)
        if not data:
            continue
        d = data.get("data") if isinstance(data, dict) else data
        if isinstance(d, dict):
            bids = d.get("bids", [])
            asks = d.get("asks", [])
            def _norm(x):
                out = []
                for i in x:
                    if isinstance(i, list) and len(i) >= 2:
                        out.append([float(i[0]), float(i[1])])
                    elif isinstance(i, dict):
                        out.append([float(i.get("price", 0)), float(i.get("amount", 0))])
                return out
            ob = {"bids": _norm(bids), "asks": _norm(asks)}
            if ob["bids"] or ob["asks"]:
                logger.info(f"✔️ LBank orderbook {symbol}: bids={len(ob['bids'])}, asks={len(ob['asks'])}")
                return ob

    logger.warning(f"No orderbook fetched for {symbol} (LBank).")
    return {}

def collect_coinglass_metrics(symbol: str) -> dict:
    # Optional stub: leave empty if not needed; filled for completeness
    if not CONFIG["COINGLASS_KEY"] or CONFIG["FORCE_DEMO"]:
        return {}
    headers = {"CG-API-KEY": CONFIG["COINGLASS_KEY"], "accept": "application/json"}
    base = CONFIG["COINGLASS_BASE"].rstrip("/")
    out = {}
    # Placeholder; adapt with your accessible endpoints if desired
    out.setdefault("oi_change_pct", 0.0)
    out.setdefault("funding_rate", 0.0)
    return out

# ----------------------------
# Analyzer
# ----------------------------
def detect_large_trades(trades: List[dict], price_hint: Optional[float], min_usd: float) -> Tuple[bool, float, str]:
    found = False
    max_usd = 0.0
    max_side = ""
    p = price_hint or (trades[0]["price"] if trades else 0.0)
    for t in trades:
        price = t["price"] or p
        usd = abs(price * t["qty"])
        side = t.get("side", "")
        if usd > max_usd:
            max_usd = usd
            max_side = side
        if usd >= min_usd:
            found = True
    return found, max_usd, max_side

def detect_orderbook_sweep(orderbook: dict, sweep_depth_usd: float, price_hint: Optional[float]) -> Tuple[bool, float]:
    if not orderbook:
        return (False, 0.0)
    bids = orderbook.get("bids", [])[:15]
    asks = orderbook.get("asks", [])[:15]
    last_price = price_hint or (bids[0][0] if bids else (asks[0][0] if asks else 0.0))
    bid_usd = sum(p * q for p, q in bids)
    ask_usd = sum(p * q for p, q in asks)
    low_depth_threshold = sweep_depth_usd
    swept = (bid_usd <= low_depth_threshold) or (ask_usd <= low_depth_threshold)
    swept_depth = min(bid_usd, ask_usd)
    return (swept, swept_depth)

def coinglass_signal(m: dict, liq_min: float) -> Tuple[bool, float, float, float, float]:
    if not m:
        return (False, 0.0, 0.0, 0.0, 0.0)
    liq_buy = float(m.get("liq_buy_usd", 0.0))
    liq_sell = float(m.get("liq_sell_usd", 0.0))
    has_spike = (liq_buy >= liq_min) or (liq_sell >= liq_min)
    oi_change = float(m.get("oi_change_pct", 0.0))
    funding = float(m.get("funding_rate", 0.0))
    return (has_spike, liq_buy, liq_sell, oi_change, funding)

def fuse_score(
    large_trade: Tuple[bool, float, str],
    sweep: Tuple[bool, float],
    cg_sig: Tuple[bool, float, float, float, float],
    pct_change: float
) -> Tuple[float, str, str]:
    (lt_found, lt_max_usd, lt_side) = large_trade
    (sw_found, sw_depth) = sweep
    (liq_spike, liq_buy, liq_sell, oi_change, funding) = cg_sig

    score = 0.0
    if lt_found:
        score += CONFIG["W_LARGE_TRADE"]
    if sw_found:
        score += CONFIG["W_SWEEP"]
    if liq_spike:
        score += CONFIG["W_LIQ"]
    if abs(oi_change) >= 1e-6:
        score += CONFIG["W_OI"] * (1.0 if oi_change > 0 else 0.5)
    if abs(pct_change) >= 1e-6:
        score += CONFIG["W_PCT"] * (1.0 if pct_change > 0 else 0.5)

    if score >= CONFIG["LABEL_STRONG"]:
        label = "strong"
    elif score >= CONFIG["LABEL_PROBABLE"]:
        label = "probable"
    elif score > 0:
        label = "weak"
    else:
        label = "neutral"

    if lt_side in ("buy", "sell"):
        side = lt_side
    elif liq_spike:
        side = "buy" if liq_buy >= liq_sell else "sell"
    else:
        side = "buy" if pct_change > 0 else ("sell" if pct_change < 0 else "")
    return score, label, side

# ----------------------------
# CoinGlass taker (4h by default) + Binance fallback
# ----------------------------
COINGLASS_API_KEY = CONFIG["COINGLASS_KEY"]

def fetch_coinglass_taker_interval(symbol_pair: str, interval: Optional[str] = None) -> Optional[dict]:
    """
    Try CoinGlass futures taker buy/sell for pair like 'BTCUSDT' with given interval (default from CONFIG['TAKER_INTERVAL']).
    Returns {'buyVol': float, 'sellVol': float, 'timestamp': int} or None.
    Designed for free/low plans: default interval '4h'.
    """
    if not COINGLASS_API_KEY:
        logger.info("CoinGlass API key not set — skipping CoinGlass taker fetch.")
        return None

    interval = interval or CONFIG.get("TAKER_INTERVAL", "4h")
    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    base = CONFIG["COINGLASS_BASE"].rstrip("/")

    endpoints = [
        f"{base}/api/futures/v2/taker-buy-sell-volume/history",
        f"{base}/api/futures/aggregated-taker-buy-sell-volume/history",
        f"{base}/api/futures/taker-buy-sell-volume/history",
    ]
    params_variants = [
        {"pair": symbol_pair, "interval": interval},
        {"symbol": symbol_pair, "interval": interval},
        {"coin": symbol_pair.replace("USDT", ""), "interval": interval},
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
                logger.info(f"CoinGlass taker {symbol_pair} {interval}: buy={buy}, sell={sell}")
                return {"buyVol": buy, "sellVol": sell, "timestamp": ts}

    logger.warning("CoinGlass taker fetch returned no usable data (check key/plan/interval).")
    return None

def compute_taker_delta_with_fallback(symbol_short: str, interval: Optional[str] = None) -> Optional[float]:
    """
    Returns taker delta in [-1,+1] using CoinGlass (preferred, interval from CONFIG; default 4h),
    else fallback to Binance USDⓈ-M endpoint (public), else None.
    """
    interval = interval or CONFIG.get("TAKER_INTERVAL", "4h")
    pair = f"{symbol_short}USDT"

    # CoinGlass first
    try:
        cg = fetch_coinglass_taker_interval(pair, interval=interval)
        if cg:
            buy, sell = cg["buyVol"], cg["sellVol"]
            denom = buy + sell
            return 0.0 if denom <= 0 else (buy - sell) / denom
    except Exception as e:
        logger.debug(f"CoinGlass delta error: {e}")

    # Binance fallback
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
        logger.debug(f"Binance fallback delta error: {e}")

    return None

# ----------------------------
# CSV Saving
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
    demo = {
        "BTC": dict(price=100000.0, pct=+0.8, lt=True, lt_max=250_000, lt_side="buy", sw=True, sw_depth=120_000, liq_b=300_000, liq_s=50_000, oi=+2.1, fund=0.01, delta=+0.35),
        "ETH": dict(price=4000.0,   pct=-0.6, lt=False, lt_max=50_000,  lt_side="",     sw=False, sw_depth=500_000, liq_b=0,       liq_s=0,      oi=-1.0, fund=-0.005, delta=-0.10),
        "BNB": dict(price=650.0,    pct=+0.2, lt=True, lt_max=120_000, lt_side="sell", sw=True, sw_depth=80_000,  liq_b=10_000,  liq_s=250_000, oi=+0.5, fund=0.0,   delta=+0.05),
    }
    for s in symbols:
        d = demo.get(s, dict(price=1.0, pct=0.0, lt=False, lt_max=0, lt_side="", sw=False, sw_depth=999999, liq_b=0, liq_s=0, oi=0.0, fund=0.0, delta=0.0))
        lt_tuple = (d["lt"], float(d["lt_max"]), d["lt_side"])
        sw_tuple = (d["sw"], float(d["sw_depth"]))
        cg_tuple = ((d["liq_b"] >= CONFIG["COINGLASS_LIQ_MIN"]) or (d["liq_s"] >= CONFIG["COINGLASS_LIQ_MIN"]),
                    float(d["liq_b"]), float(d["liq_s"]), float(d["oi"]), float(d["fund"]))
        score, label, side = fuse_score(lt_tuple, sw_tuple, cg_tuple, d["pct"])
        rows.append({
            "timestamp_utc": now,
            "symbol": s,
            "price_last": d["price"],
            "pct_change": d["pct"],
            "total_quote_5m": None,
            "max_trade_usd": d["lt_max"],
            "whale_detected": (label in ("strong", "probable", "weak")),
            "whale_side": side,
            "orderbook_sweep": d["sw"],
            "sweep_depth_usd": d["sw_depth"],
            "coinglass_liq_buy_usd": d["liq_b"],
            "coinglass_liq_sell_usd": d["liq_s"],
            "oi_change_pct": d["oi"],
            "funding_rate": d["fund"],
            "signal_score": score,
            "signal_label": label,
            "taker_delta_interval": d["delta"],
            "taker_delta_alert_interval": abs(d["delta"]) >= CONFIG["TAKER_ALERT"],
            "run_file": "",
        })
    return rows

# ----------------------------
# Main pipeline
# ----------------------------
def run_pipeline(symbols: List[str]) -> pd.DataFrame:
    rows = []
    lookback = CONFIG["LOOKBACK_MIN"]
    for sym in symbols:
        try:
            trades = collect_recent_trades_lbank(sym, lookback)
            orderbook = collect_orderbook_lbank(sym)
            cg = collect_coinglass_metrics(sym)

            price_hint = None
            if trades:
                price_hint = float(trades[-1]["price"])
            elif orderbook.get("bids") or orderbook.get("asks"):
                best_bid = orderbook.get("bids", [[0, 0]])[0][0] if orderbook.get("bids") else 0.0
                best_ask = orderbook.get("asks", [[0, 0]])[0][0] if orderbook.get("asks") else 0.0
                price_hint = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else (best_bid or best_ask or None)

            large_trade = detect_large_trades(trades, price_hint, CONFIG["TRADE_USD_MIN"])
            sweep = detect_orderbook_sweep(orderbook, CONFIG["SWEEP_DEPTH_USD"], price_hint)
            cg_sig = coinglass_signal(cg, CONFIG["COINGLASS_LIQ_MIN"])

            pct_change = 0.0  # placeholder without OHLC history
            score, label, side = fuse_score(large_trade, sweep, cg_sig, pct_change)

            total_quote_5m = 0.0
            if trades:
                now_ms = int(datetime.utcnow().timestamp() * 1000)
                five_min_ms = 5 * 60 * 1000
                for t in trades:
                    if t["ts"] and (now_ms - t["ts"] <= five_min_ms):
                        px = t["price"] or (price_hint or 0.0)
                        total_quote_5m += abs(px * t["qty"])

            # taker delta (4h default) via CoinGlass with Binance fallback
            try:
                delta = compute_taker_delta_with_fallback(sym, interval=CONFIG["TAKER_INTERVAL"])
            except Exception:
                delta = None

            row = {
                "timestamp_utc": now_utc_iso(),
                "symbol": sym,
                "price_last": float(price_hint or 0.0),
                "pct_change": pct_change,
                "total_quote_5m": total_quote_5m if total_quote_5m else None,
                "max_trade_usd": float(large_trade[1]) if large_trade else 0.0,
                "whale_detected": (score > 0),
                "whale_side": side,
                "orderbook_sweep": bool(sweep[0]),
                "sweep_depth_usd": float(sweep[1]),
                "coinglass_liq_buy_usd": float(cg_sig[1]),
                "coinglass_liq_sell_usd": float(cg_sig[2]),
                "oi_change_pct": float(cg_sig[3]),
                "funding_rate": float(cg_sig[4]),
                "signal_score": float(score),
                "signal_label": str(label),
                "taker_delta_interval": delta,
                "taker_delta_alert_interval": (abs(delta) >= CONFIG["TAKER_ALERT"]) if (delta is not None) else False,
                "run_file": "",
            }

            if row["taker_delta_alert_interval"]:
                logger.info(f"⚠️ TAKER delta ALERT ({CONFIG['TAKER_INTERVAL']}) for {sym}: {delta:.2%}")

            rows.append(row)

        except Exception as e:
            logger.error(f"Symbol {sym} pipeline error: {e}")
            logger.debug(traceback.format_exc())

    # DEMO fallback if nothing meaningful
    if not rows or all(
        (r["price_last"] == 0.0 and not r["orderbook_sweep"] and r["max_trade_usd"] == 0.0 and r["taker_delta_interval"] in (None, 0.0))
        for r in rows
    ):
        logger.warning("⚠️ No real data collected — switching to DEMO rows.")
        rows = build_demo_rows(symbols)

    df = pd.DataFrame(rows)
    return df

def main() -> int:
    try:
        symbols = CONFIG["SYMBOLS"]
        logger.info(f"🧪 Symbols: {symbols}")
        df = run_pipeline(symbols)
        out_path = save_output_csv(df)
        df["run_file"] = out_path
        df.to_csv(DATA_DIR / "latest.csv", index=False)
        logger.info(f"✅ Completed. Rows: {len(df)}")
        logger.info("\n" + df.head(min(10, len(df))).to_string(index=False))
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
