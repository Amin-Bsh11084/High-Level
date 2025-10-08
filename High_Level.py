# High_Level.py
"""
High_Level — LBank whale monitor (Trades + Orderbook Sweep + Coinglass + Scoring)
"""

import os
import sys
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

if _HAS_LOGURU:
    # پاک‌سازی هندلرهای پیش‌فرض و افزودن فایل روتیت
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
    # پیکربندی استاندارد logging با RotatingFileHandler
    import logging
    from logging.handlers import RotatingFileHandler

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stream_h = logging.StreamHandler()
    stream_h.setFormatter(fmt)

    file_h = RotatingFileHandler(str(LOG_FILE), maxBytes=1_000_000, backupCount=5)
    file_h.setFormatter(fmt)

    # از اضافه‌کردن هندلرهای تکراری جلوگیری کنیم
    if not logger.handlers:
        logger.addHandler(stream_h)
        logger.addHandler(file_h)

logger.info("🚀 High_Level started (logging configured).")
