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


# try to use loguru, fallback to stdlib logging
try:
from loguru import logger # type: ignore
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


# --- paths & state ---
os.makedirs("logs", exist_ok=True)
LOG_FILE = "logs/run_log.txt"
if _HAS_LOGURU:
try:
logger.remove()
except Exception:
pass
logger.add(LOG_FILE, rotation="1 MB", retention="7 days", enqueue=True)
else:
import logging as _logging
if not any(isinstance(h, _logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(LOG_FILE) for h in logger.handlers):
fh = _logging.FileHandler(LOG_FILE)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)


STATE_DIR = Path(".state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
raise
