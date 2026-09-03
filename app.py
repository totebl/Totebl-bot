# ========================================================================
# بوت التداول v29.0 – النسخة المبسطة (بعد تنظيف شامل)
# تم حذف: التلاعب، الأداء 30 يوم، الذكاء الاصطناعي، التوصيات، لجنة البيع، حالة السوق، الوقف المسبق
# تم تبسيط حجم الصفقة إلى نسبة ثابتة قابلة للتعديل عبر تلغرام.
# جميع الميزات الأساسية (لجنة الشراء، المراقبة، التنفيذ، الفلاتر) محفوظة.
# ========================================================================

import subprocess, sys, os, logging, time, warnings, threading, atexit, signal, re, traceback, io
import logging.handlers
from datetime import datetime, timedelta
from collections import defaultdict, deque, OrderedDict
from contextlib import contextmanager
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

def _auto_install(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        __import__(import_name)
        return True
    except ImportError:
        if os.environ.get('AUTO_INSTALL', 'true').lower() != 'true':
            print(f"⚠️ AUTO_INSTALL=false => تخطي تثبيت {package}")
            return False
        print(f"📦 تثبيت المكتبة المفقودة: {package} ...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ تم تثبيت {package}")
            return True
        except Exception as e:
            print(f"❌ فشل تثبيت {package}: {e}")
            return False

from dotenv import load_dotenv
load_dotenv()

try:
    import numpy as np
    import pandas as pd
except ImportError:
    if _auto_install("numpy") and _auto_install("pandas"):
        import numpy as np
        import pandas as pd
    else:
        raise

try:
    import requests, json
except ImportError:
    if _auto_install("requests"):
        import requests, json
    else:
        raise

try:
    from flask import Flask, jsonify, render_template_string, request, abort, session
    from flask_cors import CORS
except ImportError:
    if _auto_install("flask") and _auto_install("flask_cors"):
        from flask import Flask, jsonify, render_template_string, request, abort, session
        from flask_cors import CORS
    else:
        raise

try:
    import ccxt
except ImportError:
    if _auto_install("ccxt"):
        import ccxt
    else:
        raise

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    from flask_wtf import CSRFProtect
    from flask_wtf.csrf import generate_csrf
except ImportError:
    if _auto_install("flask-limiter") and _auto_install("flask-wtf"):
        from flask_limiter import Limiter
        from flask_limiter.util import get_remote_address
        from flask_wtf import CSRFProtect
        from flask_wtf.csrf import generate_csrf
    else:
        raise

try:
    import pytz
except ImportError:
    _auto_install("pytz")
    import pytz

try:
    import websocket
except ImportError:
    _auto_install("websocket-client")
    import websocket

warnings.filterwarnings('ignore')

# --------------------------- إعدادات التسجيل ---------------------------
log_file = "bot.log"
handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console)

# --------------------------- تطبيق Flask ---------------------------
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY')
if not app.secret_key:
    logger.warning("⚠️ FLASK_SECRET_KEY غير معروف، تم إنشاء مفتاح عشوائي.")
    app.secret_key = os.urandom(32).hex()
CORS(app)
limiter = Limiter(get_remote_address, app=app, default_limits=["500 per day", "200 per hour"], storage_uri="memory://")
csrf = CSRFProtect(app)
app.config['CSRF_COOKIE_HTTPONLY'] = False
logging.getLogger('werkzeug').setLevel(logging.INFO)

# --------------------------- إصلاح نهائي لخطأ UnicodeEncodeError ---------------------------
from werkzeug.serving import WSGIRequestHandler
WSGIRequestHandler.server_version = "Werkzeug"
WSGIRequestHandler.sys_version = ""

# --------------------------- متغيرات البيئة الأساسية ---------------------------
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD')
COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY', '')

if not ADMIN_PASSWORD:
    logger.critical("❌ ADMIN_PASSWORD مطلوب!")
    sys.exit(1)

ENABLE_TRADING = os.environ.get('ENABLE_TRADING', 'false').lower() == 'true'
TEST_MODE = os.environ.get('TEST_MODE', 'false').lower() == 'true'
BINANCE_SANDBOX = os.environ.get('BINANCE_SANDBOX', 'false').lower() == 'true'
PAPER_TRADING = os.environ.get('PAPER_TRADING', 'false').lower() == 'true'
PAPER_INITIAL_BALANCE = float(os.environ.get('PAPER_INITIAL_BALANCE', 10000.0))

MONITORING_ONLY = not ENABLE_TRADING

if TEST_MODE and BINANCE_SANDBOX:
    logger.warning("⚠️ TEST_MODE=true و BINANCE_SANDBOX=true معًا. TEST_MODE سيسود.")
    BINANCE_SANDBOX = False

# --------------------------- إعدادات التداول الأساسية ---------------------------
MAX_POSITION_PERCENT = float(os.environ.get('MAX_POSITION_PERCENT', 0.15))
COOLDOWN_HOURS_GOOD = float(os.environ.get('COOLDOWN_HOURS_GOOD', 0.5))
COOLDOWN_HOURS_BAD = float(os.environ.get('COOLDOWN_HOURS_BAD', 1.0))
MAX_EXPOSED_PERCENT = float(os.environ.get('MAX_EXPOSED_PERCENT', 1.0))
MAX_DAILY_LOSS_PERCENT_OF_EXPOSED = float(os.environ.get('MAX_DAILY_LOSS_PERCENT_OF_EXPOSED', 0.066))
COOLDOWN_HOURS_LOSS_LIMIT = float(os.environ.get('COOLDOWN_HOURS_LOSS_LIMIT', 6))
MIN_VOLUME_USD = float(os.environ.get('MIN_VOLUME_USD', 30000))
STRENGTH_THRESHOLD = 0.46   # تم التعديل
SCALP_MIN_PROFIT = 0.46     # تم التعديل
MAX_SL_PERCENT_NORMAL = float(os.environ.get('MAX_SL_PERCENT_NORMAL', 0.029))
MAX_SL_PERCENT_MEME = float(os.environ.get('MAX_SL_PERCENT_MEME', 0.066))
SCAN_INTERVAL_MINUTES = int(os.environ.get('SCAN_INTERVAL_MINUTES', 8))
TOP_CANDIDATES_COUNT = int(os.environ.get('TOP_CANDIDATES_COUNT', 30))
ACTIVE_SYMBOLS_LIMIT = int(os.environ.get('ACTIVE_SYMBOLS_LIMIT', 12))
MAX_DAILY_TRADES = int(os.environ.get('MAX_DAILY_TRADES', 45))
MIN_24H_CHANGE_PERCENT = float(os.environ.get('MIN_24H_CHANGE_PERCENT', 1.0))
MIN_24H_VOLUME_USD = float(os.environ.get('MIN_24H_VOLUME_USD', 100_000))
MIN_MARKET_CAP_USD = float(os.environ.get('MIN_MARKET_CAP_USD', 2_000_000))
NORMAL_MOMENTUM_DECAY_THRESHOLD = float(os.environ.get('NORMAL_MOMENTUM_DECAY_THRESHOLD', 0.25))
NORMAL_MOMENTUM_CHECK_MINUTES = int(os.environ.get('NORMAL_MOMENTUM_CHECK_MINUTES', 15))
NORMAL_MAX_NO_PROFIT_HOLD_MINUTES = int(os.environ.get('NORMAL_MAX_NO_PROFIT_HOLD_MINUTES', 45))
MEME_MOMENTUM_DECAY_THRESHOLD = float(os.environ.get('MEME_MOMENTUM_DECAY_THRESHOLD', 0.15))
MEME_MOMENTUM_CHECK_MINUTES = int(os.environ.get('MEME_MOMENTUM_CHECK_MINUTES', 15))
MEME_MAX_NO_PROFIT_HOLD_MINUTES = int(os.environ.get('MEME_MAX_NO_PROFIT_HOLD_MINUTES', 30))
LIMIT_ORDER_SLIPPAGE = float(os.environ.get('LIMIT_ORDER_SLIPPAGE', 0.003))
LIMIT_ORDER_TIMEOUT_BASE = int(os.environ.get('LIMIT_ORDER_TIMEOUT_BASE', 30))
LIMIT_ORDER_TIMEOUT_MEME = int(os.environ.get('LIMIT_ORDER_TIMEOUT_MEME', 45))
POST_ONLY_ORDERS = os.environ.get('POST_ONLY_ORDERS', 'false').lower() == 'true'
LIQUIDITY_CHECK_DEPTH = int(os.environ.get('LIQUIDITY_CHECK_DEPTH', 10))
LIQUIDITY_MAX_PERCENT = float(os.environ.get('LIQUIDITY_MAX_PERCENT', 0.12))
MAX_PERFORMANCE_SYMBOLS = int(os.environ.get('MAX_PERFORMANCE_SYMBOLS', 200))
MAX_RETRIES_CLOSE = int(os.environ.get('MAX_RETRIES_CLOSE', 5))
STUCK_POSITION_RETRY_MINUTES_NORMAL = int(os.environ.get('STUCK_POSITION_RETRY_MINUTES_NORMAL', 5))
STUCK_POSITION_RETRY_MINUTES_MEME = int(os.environ.get('STUCK_POSITION_RETRY_MINUTES_MEME', 2))
DAILY_LOSS_MODE = os.environ.get('DAILY_LOSS_MODE', 'net')
MAX_STUCK_RETRIES = int(os.environ.get('MAX_STUCK_RETRIES', 10))
TRAILING_ACTIVATION_NORMAL = float(os.environ.get('TRAILING_ACTIVATION_NORMAL', 0.020))
TRAILING_ACTIVATION_MEME = float(os.environ.get('TRAILING_ACTIVATION_MEME', 0.025))
TRAILING_DISTANCE_NORMAL = float(os.environ.get('TRAILING_DISTANCE_NORMAL', 0.010))
TRAILING_DISTANCE_MEME = float(os.environ.get('TRAILING_DISTANCE_MEME', 0.017))
MIN_DEPTH_USD_NORMAL = int(os.environ.get('MIN_DEPTH_USD_NORMAL', 3000))
MIN_DEPTH_USD_MEME = int(os.environ.get('MIN_DEPTH_USD_MEME', 4000))
MAX_SPREAD_NORMAL = float(os.environ.get('MAX_SPREAD_NORMAL', 0.003))
MAX_SPREAD_MEME = float(os.environ.get('MAX_SPREAD_MEME', 0.0144))
MAX_SLIPPAGE_EMERGENCY_NORMAL = float(os.environ.get('MAX_SLIPPAGE_EMERGENCY_NORMAL', 0.02))
MAX_SLIPPAGE_EMERGENCY_MEME = float(os.environ.get('MAX_SLIPPAGE_EMERGENCY_MEME', 0.05))
OVERBOUGHT_RSI_THRESHOLD = float(os.environ.get('OVERBOUGHT_RSI_THRESHOLD', 70.0))
OVERBOUGHT_STOCH_THRESHOLD = float(os.environ.get('OVERBOUGHT_STOCH_THRESHOLD', 80.0))

# --------------------------- تبريد الصفقات المتكررة (قابل للتعديل عبر تلغرام) ---------------------------
COOLDOWN_WIN_HOURS = 8.0   # تم التعديل
COOLDOWN_LOSS_MINUTES = 120.0   # تم التعديل
_symbol_cooldown_until = {}
_cooldown_lock = threading.Lock()

# --------------------------- عتبات الخسارة الوقائية (قابلة للتعديل عبر تلغرام) ---------------------------
STOP_LOSS_PARTIAL_1_PERCENT = 0.0144   # 1.44%
STOP_LOSS_PARTIAL_2_PERCENT = 0.0155   # 1.55%
STOP_LOSS_FULL_PERCENT = 0.0166        # 1.66%

# --------------------------- المتغيرات العامة ---------------------------
PAUSED = False
PAUSE_NEW_ENTRIES = False
PAUSE_ANALYSIS = False

# --------------------------- متغير التحكم في المضاعف الجديد ---------------------------
USE_1H_MULTIPLIER = True

# ========== فلتر النموذج الأول (حد أدنى) ==========
SINGLE_MODEL_FILTER_MODEL = os.environ.get('SINGLE_MODEL_FILTER_MODEL', 'rule')
SINGLE_MODEL_FILTER_THRESHOLD = float(os.environ.get('SINGLE_MODEL_FILTER_THRESHOLD', 0.11))
SINGLE_MODEL_FILTER_TIMEFRAME = os.environ.get('SINGLE_MODEL_FILTER_TIMEFRAME', '5m')
SINGLE_MODEL_FILTER_ENABLED = os.environ.get('SINGLE_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج الثاني (حد أدنى) ==========
SECOND_MODEL_FILTER_MODEL = os.environ.get('SECOND_MODEL_FILTER_MODEL', 'cfhm')
SECOND_MODEL_FILTER_THRESHOLD = 0.69   # تم التعديل
SECOND_MODEL_FILTER_TIMEFRAME = os.environ.get('SECOND_MODEL_FILTER_TIMEFRAME', '5m')
SECOND_MODEL_FILTER_ENABLED = os.environ.get('SECOND_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج الثالث (حد أدنى) ==========
THIRD_MODEL_FILTER_MODEL = os.environ.get('THIRD_MODEL_FILTER_MODEL', 'cfhm')
THIRD_MODEL_FILTER_THRESHOLD = 0.55   # تم التعديل
THIRD_MODEL_FILTER_TIMEFRAME = os.environ.get('THIRD_MODEL_FILTER_TIMEFRAME', '1h')
THIRD_MODEL_FILTER_ENABLED = os.environ.get('THIRD_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج الرابع (حد أدنى) ==========
FOURTH_MODEL_FILTER_MODEL = os.environ.get('FOURTH_MODEL_FILTER_MODEL', 'vwap_obv')
FOURTH_MODEL_FILTER_THRESHOLD = float(os.environ.get('FOURTH_MODEL_FILTER_THRESHOLD', 0.44))
FOURTH_MODEL_FILTER_TIMEFRAME = os.environ.get('FOURTH_MODEL_FILTER_TIMEFRAME', '15m')
FOURTH_MODEL_FILTER_ENABLED = os.environ.get('FOURTH_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج الخامس (حد أعلى) ==========
FIFTH_MODEL_FILTER_MODEL = os.environ.get('FIFTH_MODEL_FILTER_MODEL', 'timing')
FIFTH_MODEL_FILTER_THRESHOLD = 0.75   # تم التعديل
FIFTH_MODEL_FILTER_TIMEFRAME = os.environ.get('FIFTH_MODEL_FILTER_TIMEFRAME', '1h')
FIFTH_MODEL_FILTER_ENABLED = os.environ.get('FIFTH_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج السادس (حد أدنى - جديد) ==========
SIXTH_MODEL_FILTER_MODEL = os.environ.get('SIXTH_MODEL_FILTER_MODEL', 'vwap_obv')
SIXTH_MODEL_FILTER_THRESHOLD = 0.47   # تم التعديل
SIXTH_MODEL_FILTER_TIMEFRAME = os.environ.get('SIXTH_MODEL_FILTER_TIMEFRAME', '1h')
SIXTH_MODEL_FILTER_ENABLED = os.environ.get('SIXTH_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== فلتر النموذج السابع (حد أدنى - جديد) ==========
SEVENTH_MODEL_FILTER_MODEL = os.environ.get('SEVENTH_MODEL_FILTER_MODEL', 'timing')
SEVENTH_MODEL_FILTER_THRESHOLD = 0.25   # تم التعديل
SEVENTH_MODEL_FILTER_TIMEFRAME = os.environ.get('SEVENTH_MODEL_FILTER_TIMEFRAME', '15m')
SEVENTH_MODEL_FILTER_ENABLED = os.environ.get('SEVENTH_MODEL_FILTER_ENABLED', 'true').lower() == 'true'

# ========== نظام تتبع الأرباح الجديد (يفعل فوراً) ==========
TRAILING_DISTANCE_PERCENT = 0.04   # 4.00%

# ========== أوزان الأطر الزمنية (قابلة للتعديل عبر تلغرام) ==========
WEIGHT_5M = 40.0   # تم التعديل
WEIGHT_15M = 12.0  # تم التعديل
WEIGHT_1H = 48.0   # تم التعديل

# ===== الحد الأعلى الموحد للثقة (جديد) =====
UPPER_THRESHOLD_GLOBAL = 0.85

# ===== متغيرات التحديث: تذكر آخر 12 رمزاً تم تحليلها =====
_last_analyzed_candidates = []

# ===== متغير حجم الصفقة الثابت (قابل للتعديل عبر تلغرام) =====
POSITION_SIZE_PERCENT = 0.98   # 98% افتراضياً

# ===== المتغيرات العامة الأخرى =====
_last_scan_candidates = []
daily_loss_cooldown_until = None
_last_successful_balance_time = 0
_balance_failure_paused = False
_balance_failure_start_time = 0
_balance_retry_count = 0
MAX_BALANCE_RETRIES = 20
_last_error_report_times = {}
MIN_REPORT_INTERVAL = 300
_last_analysis_time = datetime.now()
_last_analysis_time_lock = threading.Lock()
_last_processing_lock_released = time.time()

TRADE_LOG_FILE = "data/trade_log.csv"
STATE_FILE = "data/bot_state.json"
STATE_BAK_FILE = "data/bot_state.bak"
TELEGRAM_LAST_ID_FILE = "data/telegram_last_id.txt"
SECURITY_LOG = "data/security.log"
STUCK_POSITIONS_LOG = "data/stuck_positions.log"
os.makedirs("data", exist_ok=True)
os.makedirs("models", exist_ok=True)
os.makedirs("static", exist_ok=True)

trade_log_handler = logging.handlers.RotatingFileHandler(TRADE_LOG_FILE, maxBytes=10_000_000, backupCount=5)
trade_log_handler.setFormatter(logging.Formatter('%(asctime)s,%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
trade_logger = logging.getLogger('trade_logger')
trade_logger.addHandler(trade_log_handler)
trade_logger.setLevel(logging.INFO)

open_positions = {}
_local_pending_symbols = set()
_exchange_pending_symbols = set()
_daily_loss_tracker = 0.0
_daily_trades_count = 0
_daily_trades_date = None
_daily_winning_trades = 0
_daily_losing_trades = 0
_daily_biggest_win = 0.0
_daily_biggest_loss = 0.0
_daily_most_traded = defaultdict(int)
_daily_total_holding_time_win = 0.0
_daily_total_holding_time_loss = 0.0
_daily_holding_count_win = 0
_daily_holding_count_loss = 0
last_analysis_time = datetime.now()
_global_state_lock = threading.RLock()
_processing_lock = threading.Lock()
_state_lock = threading.RLock()
_ohlcv_cache = OrderedDict()
_ohlcv_cache_max = 150
_features_cache = OrderedDict()
_features_cache_max = 200
_features_cache_lock = threading.Lock()
_last_ping_time = datetime.now()
_market_cap_cache = {}
_market_cap_cache_lock = threading.Lock()
scanner = None
_last_telegram_update_id = 0
_cache_lock = threading.Lock()
_essential_threads = {}
coingecko_session = requests.Session()
coingecko_session.headers.update({"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
STALE_DATA_MAX_AGE = int(os.environ.get('STALE_DATA_MAX_AGE', 120))
STARTUP_DELAY_SECONDS = 60
STARTUP_REDUCED_LIMIT_DURATION = 300
STARTUP_REDUCED_SYMBOLS = 5
INITIAL_SCAN_DELAY = 180
_bot_start_time = time.time()
_initial_warmup_done = False
_last_telegram_failure_time = 0
TELEGRAM_BACKOFF = 300
STOP_LOSS_MULTIPLIER_NORMAL = 1.5
STOP_LOSS_MULTIPLIER_MEME = 2.0
TIMEFRAMES = {'primary': '15m', 'confirm_1': '5m', 'confirm_2': '1h'}
SCALP_TIMEFRAME = '15m'
BASE_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']
BASE_SYMBOLS_SET = set(BASE_SYMBOLS)
VOLATILITY_THRESHOLD = 0.03
MEME_PRICE_CHANGE_24H = 5.0
TAKE_PROFIT_LEVELS_NORMAL = [0.015, 0.020, 0.025, 0.030, 0.035, 0.040, 0.050]
TAKE_PROFIT_LEVELS_MEME = [0.015, 0.022, 0.030, 0.041, 0.051, 0.066, 0.080]
TAKE_PROFIT_PERCENTS_NORMAL = [0.15, 0.15, 0.15, 0.15, 0.20, 0.50, 1.00]
TAKE_PROFIT_PERCENTS_MEME = [0.15, 0.15, 0.15, 0.15, 0.20, 0.50, 1.00]
LOCK_TIMEOUT_SECONDS = 300
FETCH_OHLCV_TIMEOUT = int(os.environ.get('FETCH_OHLCV_TIMEOUT', 200))
FILTER_LIQUIDITY_ENABLED = True
FILTER_MARKET_CAP_ENABLED = True
FILTER_VOLUME_24H_ENABLED = True
FILTER_CHANGE_24H_ENABLED = True
FILTER_HOUR_CANDLE_ENABLED = True
CURRENT_BUY_COMMITTEE_MULTIPLIER = 1.0
_analysis_failures = 0
_MAX_CONSECUTIVE_FAILURES = 3
_last_force_unlock_time = 0
_MIN_TIME_BETWEEN_FORCE_UNLOCKS = 60

# ------------------- متغيرات auto_recovery -------------------
_auto_recovery_failures = 0
_AUTO_RECOVERY_THRESHOLD = 5
_AUTO_RECOVERY_LOCK = threading.Lock()

# ------------------- المتغيرات الديناميكية الجديدة (للتحكم عبر تلغرام) -------------------
CUSTOM_ACTIVE_SYMBOLS_LIMIT = 12   # تم التعديل
CUSTOM_MAX_EXPOSED_PERCENT = MAX_EXPOSED_PERCENT   # 1.0
CUSTOM_MAX_DAILY_TRADES = 45   # تم التعديل
CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP = 60   # تم التعديل

# ------------------- متغيرات جديدة للتحكم في التحول من Limit إلى Market -------------------
LIMIT_TO_MARKET_TIMEOUT = int(os.environ.get('LIMIT_TO_MARKET_TIMEOUT', 10))
ORDER_POLL_INTERVAL = float(os.environ.get('ORDER_POLL_INTERVAL', 1.0))
MARKET_ORDER_FALLBACK = os.environ.get('MARKET_ORDER_FALLBACK', 'true').lower() == 'true'
POSITION_MONITOR_INTERVAL = float(os.environ.get('POSITION_MONITOR_INTERVAL', 1.0))

# ------------------- متغير لتذكر آخر قيمة لعدد الرموز (للكشف عن تغيير من 0 إلى موجب) -------------------
_prev_active_symbols_limit = CUSTOM_ACTIVE_SYMBOLS_LIMIT

# --------------------------- دوال مساعدة أساسية ---------------------------
def escape_html(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def force_unlock():
    global _processing_lock, _last_processing_lock_released, _last_force_unlock_time, _analysis_failures
    now = time.time()
    if now - _last_force_unlock_time < _MIN_TIME_BETWEEN_FORCE_UNLOCKS:
        return
    _last_force_unlock_time = now
    logger.warning("⚠️ محاولة تحرير قسري للقفل _processing_lock")
    try:
        while _processing_lock.locked():
            try:
                _processing_lock.release()
                logger.info("✅ تم تحرير القفل بنجاح")
            except RuntimeError:
                break
            except Exception as e:
                logger.error(f"خطأ أثناء التحرير القسري: {e}")
                break
        _last_processing_lock_released = time.time()
        _analysis_failures = 0
    except Exception as e:
        logger.error(f"فشل التحرير القسري: {e}")

def send_telegram(text):
    global _last_telegram_failure_time
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("⚠️ تلغرام غير مهيأ")
        return
    if time.time() - _last_telegram_failure_time < TELEGRAM_BACKOFF:
        return
    clean = re.sub(r'<(?!b>|/b>|i>|/i>|pre>|/pre>|code>|/code>|a\s|/a>|br\s?/?>)[^>]*>', '', text)
    clean = escape_html(clean)
    clean = clean.replace('&lt;b&gt;', '<b>').replace('&lt;/b&gt;', '</b>')
    clean = clean.replace('&lt;i&gt;', '<i>').replace('&lt;/i&gt;', '</i>')
    clean = clean.replace('&lt;pre&gt;', '<pre>').replace('&lt;/pre&gt;', '</pre>')
    for i in range(0, len(clean), 3800):
        part = clean[i:i+3800]
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": part, "parse_mode": "HTML"},
                timeout=15
            )
            if resp.status_code == 400:
                _last_telegram_failure_time = time.time()
                return
        except Exception as e:
            logger.error(f"خطأ في إرسال تلغرام: {e}")

def send_telegram_photo(buf, caption=""):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        files = {'photo': buf}
        data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': caption}
        resp = requests.post(url, files=files, data=data, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"فشل إرسال الصورة: {resp.text}")
    except Exception as e:
        logger.error(f"خطأ في إرسال الصورة: {e}")

def _set_paused(state: bool):
    global PAUSED
    with _state_lock:
        PAUSED = state

def _is_paused():
    with _state_lock:
        return PAUSED

def generate_error_report(error_type, component, details="", traceback_str=""):
    global _last_error_report_times
    now = time.time()
    key = (error_type, component)
    if key in _last_error_report_times and (now - _last_error_report_times[key]) < MIN_REPORT_INTERVAL:
        return
    _last_error_report_times[key] = now
    with _global_state_lock:
        positions_count = len(open_positions)
        paused = _is_paused()
        total_equity = get_total_equity()
        daily_loss = _daily_loss_tracker
        daily_trades = _daily_trades_count
    threads_status = {}
    for name, info in _essential_threads.items():
        t = info['thread']
        threads_status[name] = "حي" if t and t.is_alive() else "ميت"
    processing_locked = _processing_lock.locked()
    with _last_analysis_time_lock:
        last_analysis = _last_analysis_time.isoformat() if _last_analysis_time else "لم يحدث بعد"
    report = f"""<b>🚨 تقرير خطأ تلقائي</b>
<b>النوع:</b> {error_type}
<b>المكون:</b> {component}
<b>الوقت:</b> {datetime.now().isoformat()}
<b>📋 الحالة:</b>
• متوقف: {paused}
• مراكز مفتوحة: {positions_count}
• صفقات اليوم: {daily_trades}
• خسارة اليوم: ${daily_loss:.2f}
• الرصيد الكلي: ${total_equity:.2f}
• آخر تحليل: {last_analysis}
<b>🔧 الأقفال:</b> {'قفل التحليل محتجز' if processing_locked else 'لا أقفال'}
<b>🧵 الخيوط:</b>
{chr(10).join(f'• {k}: {v}' for k, v in threads_status.items())}
<b>⚠️ التفاصيل:</b>
<pre>{escape_html(details[:500])}</pre>"""
    if traceback_str:
        report += f"\n<b>📜 التتبع:</b>\n<pre>{escape_html(traceback_str[:800])}</pre>"
    send_telegram(report)

def cleanup_error_reports():
    global _last_error_report_times
    now = time.time()
    keys_to_delete = [k for k, t in _last_error_report_times.items() if now - t > 3600]
    for k in keys_to_delete:
        del _last_error_report_times[k]
    if len(_last_error_report_times) > 500:
        items = sorted(_last_error_report_times.items(), key=lambda x: x[1])
        for k, _ in items[:250]:
            del _last_error_report_times[k]

# --------------------------- كلاس BotStats ---------------------------
class BotStats:
    def __init__(self):
        self.total_pnl_usdt = 0.0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_trades = 0
        self.daily_pnl = 0.0
        self.weekly_pnl = 0.0
        self.weekly_wins = 0
        self.weekly_losses = 0
        self.last_balance = PAPER_INITIAL_BALANCE if PAPER_TRADING else 10000.0
        self.start_time = datetime.now()
        self.last_valid_tickers = {}
        self.last_tickers_time = 0
        self.equity_curve = deque(maxlen=1000)
        self.symbol_performance = {}
        self.last_week_number = datetime.now().isocalendar()[1]

    def add_equity_point(self, total_equity):
        self.equity_curve.append({'time': datetime.now().isoformat(), 'equity': total_equity})

bot_stats = BotStats()

# --------------------------- كلاس Position ---------------------------
class Position:
    def __init__(self, symbol, side, size, entry, atr, sl, tp, sym_type, pred, conf, regime, tp_levels=None, ai_approved=False,
                 scores_5m=None, scores_15m=None, scores_1h=None,
                 weighted_score_5m=None, weighted_score_15m=None, weighted_score_1h=None,
                 final_score=None):
        self.symbol = symbol
        self.side = side
        self.total_size = size
        self.remaining_size = size
        self.entry_price = entry
        self.highest_price = entry
        self.lowest_price = entry
        self.stop_loss = sl
        self.symbol_type = sym_type
        config = get_scalp_config(sym_type)
        if tp_levels:
            self.take_profit_levels = tp_levels
        else:
            self.take_profit_levels = []
            for tp_pct, pct_close in zip(config['take_profit_levels'], config['take_profit_percents']):
                target = entry * (1+tp_pct) if side=='buy' else entry * (1-tp_pct)
                self.take_profit_levels.append([target, pct_close])
        self.trailing_stop = None
        self.trailing_activated = False
        self.atr = atr
        self.open_time = datetime.now()
        self.closed_pnl = 0.0
        self.pred = pred
        self.confidence = conf
        self.regime = regime
        self.initial_momentum = None
        self.entry_momentum_time = None
        self.momentum_decay_threshold = config['momentum_decay_threshold']
        self.momentum_check_minutes = config['momentum_check_minutes']
        self.max_no_profit_minutes = config['max_no_profit_minutes']
        self.last_fail_time = None
        self.retry_count = 0
        self.crash_monitor_start = None
        self.lowest_drop = 0.0
        self._closing = False
        self.last_target_hit_time = None
        self.last_target_hit_index = -1
        self.sold_at_15 = False
        self.sold_at_20 = False
        self.tp_hit_count = 0
        self.ai_approved = ai_approved
        self.scores_5m = scores_5m or {}
        self.scores_15m = scores_15m or {}
        self.scores_1h = scores_1h or {}
        self.weighted_score_5m = weighted_score_5m
        self.weighted_score_15m = weighted_score_15m
        self.weighted_score_1h = weighted_score_1h
        self.final_score = final_score
        self.trade_id = None
        self._calc_initial_momentum()

    def _calc_initial_momentum(self):
        try:
            df = fetch_ohlcv_retry(self.symbol, TIMEFRAMES['primary'], limit=50)
            if len(df)<15: return
            lookback = max(1, int(self.momentum_check_minutes / 15))
            cur = df['close'].iloc[-1]
            past = df['close'].iloc[-lookback-1]
            self.initial_momentum = (cur - past)/past
            self.entry_momentum_time = datetime.now()
        except Exception as e:
            logger.debug(f"فشل حساب الزخم الابتدائي لـ {self.symbol}: {e}")

    def update(self, cur_price):
        if cur_price<=0: return 0
        if self.side=='buy':
            self.highest_price = max(self.highest_price, cur_price)
            profit = (cur_price - self.entry_price)/self.entry_price
        else:
            self.lowest_price = min(self.lowest_price, cur_price)
            profit = (self.entry_price - cur_price)/self.entry_price
        self.trailing_activated = True
        if self.side=='buy':
            new_stop = cur_price * (1 - TRAILING_DISTANCE_PERCENT)
            if self.trailing_stop is None or new_stop > self.trailing_stop:
                self.trailing_stop = new_stop
        else:
            new_stop = cur_price * (1 + TRAILING_DISTANCE_PERCENT)
            if self.trailing_stop is None or new_stop < self.trailing_stop:
                self.trailing_stop = new_stop
        return profit

# --------------------------- دوال التوازن والرصيد ---------------------------
def get_total_equity():
    if TEST_MODE or not ENABLE_TRADING:
        with _global_state_lock:
            return bot_stats.last_balance
    exchange = get_active_exchange()
    for attempt in range(3):
        try:
            if PAPER_TRADING:
                with _global_state_lock:
                    free = bot_stats.last_balance
                    pos_list = list(open_positions.items())
                pos_val = 0.0
                for sym, p in pos_list:
                    ws_data = ws_manager.get_ticker(sym)
                    if ws_data and ws_data.get('price', 0) > 0:
                        price = ws_data['price']
                    else:
                        ticker = None
                        for retry in range(3):
                            ticker = fetch_ticker_with_retry(sym, max_retries=1)
                            if ticker and ticker.get('last', 0) > 0:
                                price = ticker['last']
                                break
                            if retry < 2:
                                time.sleep(5)
                        else:
                            price = p.entry_price
                            logger.warning(f"⚠️ فشل جلب السعر الحالي لـ {sym} بعد 3 محاولات، استخدام سعر الدخول {price:.8f}")
                    pos_val += p.remaining_size * price
                total = free + pos_val
                logger.debug(f"📊 PAPER_EQUITY: free={free:.2f}, pos_val={pos_val:.2f}, total={total:.2f}")
                return total
            else:
                bal = exchange.fetch_balance()
                free = bal.get('USDT', {}).get('free', 0.0)
                with _global_state_lock:
                    pos_list = list(open_positions.items())
                pos_val = 0.0
                for sym, p in pos_list:
                    ws_data = ws_manager.get_ticker(sym)
                    if ws_data and ws_data.get('price', 0) > 0:
                        price = ws_data['price']
                    else:
                        ticker = None
                        for retry in range(3):
                            ticker = fetch_ticker_with_retry(sym, max_retries=1)
                            if ticker and ticker.get('last', 0) > 0:
                                price = ticker['last']
                                break
                            if retry < 2:
                                time.sleep(5)
                        else:
                            price = p.entry_price
                            logger.warning(f"⚠️ فشل جلب السعر الحقيقي لـ {sym}، استخدام سعر الدخول {price:.8f}")
                    pos_val += p.remaining_size * price
                total = free + pos_val
                if total > 0 and not PAPER_TRADING:
                    with _global_state_lock:
                        bot_stats.last_balance = total
                return total
        except Exception as e:
            logger.warning(f"محاولة {attempt+1} لجلب equity فشلت: {e}")
            time.sleep(2)
    with _global_state_lock:
        return bot_stats.last_balance

def get_real_balance_usdt(max_retries=5, delay=3.0, silent=True):
    global _last_successful_balance_time, _balance_failure_paused, _balance_failure_start_time, _balance_retry_count
    if PAPER_TRADING or TEST_MODE or not ENABLE_TRADING:
        with _global_state_lock:
            return bot_stats.last_balance
    exchange = get_active_exchange()
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed(weight=20)
            bal = exchange.fetch_balance()
            usdt = bal.get('USDT', {}).get('free', 0.0)
            if usdt is not None:
                with _state_lock:
                    bot_stats.last_balance = usdt
                    _last_successful_balance_time = time.time()
                    if _balance_failure_paused:
                        _balance_failure_paused = False
                        _set_paused(False)
                        _balance_retry_count = 0
                        if not silent:
                            send_telegram("✅ عاد الاتصال بالرصيد. استؤنف التداول تلقائياً.")
                        logger.info("✅ تم استعادة الرصيد، إعادة تفعيل التداول تلقائياً")
                return usdt
        except Exception as e:
            logger.warning(f"محاولة {attempt+1}/{max_retries} لجلب الرصيد فشلت: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    with _state_lock:
        if _last_successful_balance_time == 0 or (time.time() - _last_successful_balance_time) > 120:
            if not _balance_failure_paused:
                _balance_failure_paused = True
                _balance_failure_start_time = time.time()
                _set_paused(True)
                if not silent:
                    send_telegram("⚠️ فشل جلب الرصيد لأكثر من دقيقتين. إيقاف مؤقت مع محاولة استعادة تلقائية.")
                generate_error_report("فشل_اتصال", "رصيد", "فشل get_real_balance_usdt لأكثر من دقيقتين")
    return None

def fetch_real_balance_with_retry(timeout_seconds=120, retry_interval=5, silent=False):
    start_time = time.time()
    attempt = 0
    last_log_time = 0
    while time.time() - start_time < timeout_seconds:
        attempt += 1
        balance = get_real_balance_usdt(max_retries=1, delay=0, silent=True)
        if balance is not None:
            if not silent:
                send_telegram(f"✅ تم جلب الرصيد الحقيقي بنجاح (بعد {attempt} محاولة): ${balance:.2f} USDT")
            return balance
        if time.time() - last_log_time > 30:
            logger.info(f"⏳ جاري محاولة جلب الرصيد الحقيقي... ({attempt} محاولة)")
            last_log_time = time.time()
        time.sleep(retry_interval)
    if not silent:
        send_telegram(f"❌ فشل جلب الرصيد الحقيقي بعد {timeout_seconds} ثانية و {attempt} محاولة. تأكد من اتصال API.")
    return None

def _get_balance_no_pause(max_retries=3, delay=2.0, silent=True):
    if PAPER_TRADING or TEST_MODE or not ENABLE_TRADING:
        with _global_state_lock:
            return bot_stats.last_balance
    exchange = get_active_exchange()
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed(weight=20)
            bal = exchange.fetch_balance()
            usdt = bal.get('USDT', {}).get('free', 0.0)
            if usdt > 0:
                with _state_lock:
                    bot_stats.last_balance = usdt
                return usdt
        except Exception as e:
            if not silent:
                logger.warning(f"محاولة {attempt+1} لجلب الرصيد (بدون إيقاف) فشلت: {e}")
            time.sleep(delay)
    return None

def _safe_fetch_balance_after_trade(attempts=10, delay=3.0, silent=True):
    if PAPER_TRADING or TEST_MODE or not ENABLE_TRADING:
        with _global_state_lock:
            return bot_stats.last_balance
    for i in range(attempts):
        bal = _get_balance_no_pause(max_retries=1, silent=True)
        if bal is not None and bal > 0:
            return bal
        logger.warning(f"⚠️ فشل تحديث الرصيد بعد الصفقة (محاولة {i+1}/{attempts})")
        if i < attempts - 1:
            time.sleep(delay * (i+1))
    if not silent:
        send_telegram("⚠️ فشل تحديث الرصيد بعد الصفقة بعد عدة محاولات. قد يكون الرصيد غير دقيق.")
    return None

def _should_simulate():
    return (not ENABLE_TRADING) or TEST_MODE or PAPER_TRADING

def get_fee_rate(symbol, sym_type='normal'):
    return 0.001

def calculate_net_pnl(symbol, entry_price, fill_price, filled_size, side, sym_type='normal'):
    fee_rate = get_fee_rate(symbol, sym_type)
    fee_sell = (fill_price * filled_size) * fee_rate
    if side == 'buy':
        gross_pnl = (fill_price - entry_price) * filled_size
    else:
        gross_pnl = (entry_price - fill_price) * filled_size
    return gross_pnl - fee_sell

# --------------------------- إعداد Binance exchange ---------------------------
primary_exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'rateLimit': 3000,
    'timeout': 10000,
    'options': {
        'defaultType': 'spot',
        'recvWindow': 60000,
        'adjustForTimeDifference': True,
        'useServerTime': True,
    },
})
primary_exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
if BINANCE_SANDBOX:
    primary_exchange.set_sandbox_mode(True)
    logger.info("✅ وضع الحماية - Binance Testnet")
else:
    logger.info("✅ المنصة الحقيقية")
if PAPER_TRADING:
    logger.info("📝 وضع المحاكاة الورقية النشط")
elif TEST_MODE:
    logger.info("🧪 وضع المحاكاة المحلية")
elif not ENABLE_TRADING:
    logger.info("📊 وضع المراقبة")

def get_active_exchange():
    return primary_exchange

# --------------------------- تحميل الأسواق مع صبر شديد ---------------------------
def load_markets_with_retry(max_retries=3, initial_delay=30):
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed(weight=20)
            if attempt == 0:
                wait = initial_delay
            else:
                wait = min(180, 30 * (2 ** (attempt - 1)))
            logger.info(f"⏳ انتظار {wait} ثانية قبل المحاولة {attempt+1}/{max_retries} لتحميل الأسواق...")
            time.sleep(wait)
            primary_exchange.load_markets()
            if 'BTC/USDT' in primary_exchange.markets:
                logger.info("✅ تم تحميل الأسواق بنجاح")
                send_telegram("✅ تم تحميل بيانات الأسواق من Binance بنجاح")
                return True
            else:
                raise Exception("BTC/USDT غير موجود")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"⚠️ فشل تحميل الأسواق (محاولة {attempt+1}/{max_retries}): {e}")
            if "418" in error_msg or "banned" in error_msg or "too many" in error_msg or "Way too much" in error_msg:
                logger.critical(f"🚨 IP لا يزال محظورًا. الانتظار 5 دقائق ثم إعادة المحاولة.")
                send_telegram(f"⚠️ Binance لا يزال يحظر IP. انتظر 5 دقائق... (محاولة {attempt+1}/{max_retries})")
                time.sleep(300)
                continue
            if attempt < max_retries - 1:
                continue
    logger.critical("❌ فشل تحميل الأسواق بعد كل المحاولات.")
    send_telegram("❌ فشل تحميل الأسواق. البوت سيعمل لكن بدون بيانات أسواق صحيحة.")
    return False

# --------------------------- WebSocket Manager مع آلية إعادة تشغيل قوية + تحديث المرشحين عبر WebSocket ---------------------------
class SimpleWebSocketManager:
    def __init__(self):
        self.ticker_cache = {}
        self.lock = threading.Lock()
        self.ws = None
        self.running = False
        self.thread = None
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.last_candidates_update = 0
        self.candidates_update_interval = 600

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            with self.lock:
                for item in data:
                    symbol_raw = item.get('s', '')
                    if not symbol_raw.endswith('USDT'):
                        continue
                    symbol = symbol_raw.replace('USDT', '/USDT')
                    self.ticker_cache[symbol] = {
                        'price': float(item.get('c', 0)),
                        'change': float(item.get('P', 0)),
                        'volume': float(item.get('q', 0)),
                        'last': float(item.get('c', 0)),
                        'quoteVolume': float(item.get('q', 0)),
                        'percentage': float(item.get('P', 0)),
                        'timestamp': time.time()
                    }
                now = time.time()
                if now - self.last_candidates_update >= self.candidates_update_interval:
                    self._update_candidates_from_ws()
                    self.last_candidates_update = now
        except Exception as e:
            logger.debug(f"خطأ في معالجة رسالة WebSocket: {e}")

    def _update_candidates_from_ws(self):
        global scanner
        if scanner is None:
            return
        tickers = self.get_all_tickers()
        if not tickers or len(tickers) < 50:
            logger.debug("⚠️ بيانات WebSocket غير كافية لتحديث المرشحين، تخطي.")
            return
        try:
            filtered = []
            for sym, data in tickers.items():
                if not sym.endswith('/USDT'):
                    continue
                chg = data.get('percentage', 0.0)
                vol = data.get('quoteVolume', 0.0)
                if FILTER_CHANGE_24H_ENABLED and chg < MIN_24H_CHANGE_PERCENT:
                    continue
                if FILTER_VOLUME_24H_ENABLED and vol < max(MIN_24H_VOLUME_USD, MIN_VOLUME_USD):
                    continue
                if sym not in BASE_SYMBOLS_SET and FILTER_MARKET_CAP_ENABLED:
                    cap = get_market_cap_from_coingecko(sym, data)
                    if cap < MIN_MARKET_CAP_USD:
                        continue
                current_price = data.get('last', 1)
                hl = 1.0
                score = min(chg/8,6) + min(np.log10(max(vol,1))/5,5) + min((hl-1)*40,3)
                filtered.append((sym, score))
            filtered.sort(key=lambda x:x[1], reverse=True)
            if filtered:
                scanner.candidates = [x[0] for x in filtered[:TOP_CANDIDATES_COUNT]]
                scanner.scores = {x[0]:round(x[1],1) for x in filtered[:TOP_CANDIDATES_COUNT]}
                scanner.last_scan = time.time()
                logger.info(f"🔄 تم تحديث المرشحين عبر WebSocket ({len(scanner.candidates)} رمز)")
                global _last_scan_candidates
                new5 = scanner.candidates[:5]
                if new5 and new5 != _last_scan_candidates:
                    msg = "<b>🔍 تحديث تلقائي (WebSocket) - أفضل 5 فرص</b>\n" + "\n".join(f"{sym} ({scanner.scores[sym]:.1f})" for sym in new5)
                    send_telegram(msg)
                    _last_scan_candidates = new5
        except Exception as e:
            logger.error(f"خطأ في تحديث المرشحين عبر WebSocket: {e}")

    def on_error(self, ws, error):
        logger.error(f"⚠️ خطأ في WebSocket: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"🔌 WebSocket مغلق (code={close_status_code}). إعادة محاولة الاتصال بعد {self.reconnect_delay} ثانية...")
        if self.running:
            time.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)
            self.start()

    def on_open(self, ws):
        logger.info("✅ WebSocket متصل بنجاح (دفتر الأوامر الكامل)")
        self.reconnect_delay = 5

    def start(self):
        if self.running:
            return
        self.running = True
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws/!ticker@arr",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.thread = threading.Thread(target=self._run_forever, daemon=True)
        self.thread.start()
        logger.info("🌐 بدء تشغيل WebSocket Manager (websocket-client)")

    def _run_forever(self):
        while self.running:
            try:
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"خطأ في تشغيل WebSocket: {e}")
            if self.running:
                wait = min(self.reconnect_delay, 60)
                logger.info(f"🔄 إعادة محاولة WebSocket بعد {wait} ثانية...")
                time.sleep(wait)
                self.reconnect_delay = min(self.reconnect_delay * 1.5, 60)

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        logger.info("🛑 تم إيقاف WebSocket Manager")

    def is_alive(self):
        return self.running and self.thread and self.thread.is_alive()

    def restart(self):
        logger.warning("🔄 إعادة تشغيل WebSocket Manager...")
        self.stop()
        time.sleep(2)
        self.start()

    def get_ticker(self, symbol):
        with self.lock:
            data = self.ticker_cache.get(symbol)
            if data and (time.time() - data['timestamp']) < 10:
                return data
        return None

    def get_all_tickers(self):
        with self.lock:
            result = {}
            now = time.time()
            for sym, data in self.ticker_cache.items():
                if now - data['timestamp'] < 30:
                    result[sym] = {
                        'last': data['price'],
                        'percentage': data['change'],
                        'quoteVolume': data['volume']
                    }
            return result

ws_manager = SimpleWebSocketManager()

# --------------------------- Rate Limiter (معدل للوزن) ---------------------------
class RateLimiter:
    def __init__(self, max_weight=1000, period=60):
        self.calls = deque()
        self.max_weight = max_weight
        self.period = period
        self.lock = threading.Lock()

    def wait_if_needed(self, weight=1):
        with self.lock:
            now = time.time()
            while self.calls and self.calls[0][0] < now - self.period:
                self.calls.popleft()
            total_weight = sum(w for _, w in self.calls)
            remaining = self.max_weight - total_weight
            if remaining < 50:
                sleep_time = self.period - (now - self.calls[0][0]) + 0.5
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.calls.clear()
            elif total_weight + weight > self.max_weight:
                sleep_time = self.period - (now - self.calls[0][0])
                if sleep_time > 0:
                    time.sleep(sleep_time + 0.1)
            self.calls.append((time.time(), weight))

rest_rate_limiter = RateLimiter(max_weight=1000, period=60)

# --------------------------- دوال التيكرات (تعتمد على WebSocket أولاً) ---------------------------
def fetch_ticker_with_retry(symbol, max_retries=2):
    ticker = ws_manager.get_ticker(symbol)
    if ticker and ticker.get('price', 0) > 0:
        return {'last': ticker['price'], 'percentage': ticker.get('change',0), 'quoteVolume': ticker.get('volume',0)}
    exchange = get_active_exchange()
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed(weight=1)
            ticker = exchange.fetch_ticker(symbol)
            if ticker and ticker.get('last') is not None:
                return ticker
        except Exception as e:
            logger.warning(f"جلب السعر {symbol} محاولة {attempt+1}: {e}")
            if "418" in str(e) or "banned" in str(e):
                try:
                    retry_after = int(exchange.last_response_headers.get('Retry-After', 600))
                except:
                    retry_after = 600
                time.sleep(retry_after + random.randint(0, 30))
            time.sleep(0.5*(attempt+1))
    return None

def fetch_tickers_with_retry(max_retries=2):
    tickers = ws_manager.get_all_tickers()
    if tickers and len(tickers) > 100:
        with _global_state_lock:
            bot_stats.last_valid_tickers = dict(tickers)
            bot_stats.last_tickers_time = time.time()
        return tickers
    exchange = get_active_exchange()
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed(weight=40)
            tickers = exchange.fetch_tickers()
            if tickers:
                for sym in tickers:
                    if tickers[sym].get('percentage') is None:
                        tickers[sym]['percentage'] = 0.0
                    if tickers[sym].get('quoteVolume') is None:
                        tickers[sym]['quoteVolume'] = 0.0
                    if tickers[sym].get('last') is None:
                        tickers[sym]['last'] = 0.0
                with _global_state_lock:
                    bot_stats.last_valid_tickers = dict(tickers)
                    bot_stats.last_tickers_time = time.time()
                return tickers
        except Exception as e:
            logger.warning(f"جلب الأسعار محاولة {attempt+1}: {e}")
            if "418" in str(e) or "banned" in str(e):
                try:
                    retry_after = int(exchange.last_response_headers.get('Retry-After', 600))
                except:
                    retry_after = 600
                time.sleep(retry_after + random.randint(0, 30))
            time.sleep(2**attempt)
    with _global_state_lock:
        if bot_stats.last_valid_tickers:
            age = time.time() - bot_stats.last_tickers_time
            if age < STALE_DATA_MAX_AGE:
                logger.warning(f"⚠️ استخدام بيانات قديمة (عمرها {age:.0f} ثانية)")
                return dict(bot_stats.last_valid_tickers)
    return {}

def get_market_cap_from_coingecko(symbol, ticker_data=None):
    try:
        coin = symbol.split('/')[0].lower()
    except:
        return 0
    now = time.time()
    with _market_cap_cache_lock:
        if coin in _market_cap_cache:
            entry = _market_cap_cache[coin]
            if (now - entry['timestamp']) < 86400:
                return entry['value']
    for attempt in range(1):
        try:
            headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
            if COINGECKO_API_KEY:
                if COINGECKO_API_KEY.startswith('CG-') or len(COINGECKO_API_KEY) > 32:
                    headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
                    url = f"https://pro-api.coingecko.com/api/v3/coins/{coin}"
                else:
                    url = f"https://api.coingecko.com/api/v3/coins/{coin}?x_cg_demo_api_key={COINGECKO_API_KEY}"
            else:
                url = f"https://api.coingecko.com/api/v3/coins/{coin}"
            resp = coingecko_session.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                cap = data.get('market_data', {}).get('market_cap', {}).get('usd', 0)
                if cap > 0:
                    with _market_cap_cache_lock:
                        _market_cap_cache[coin] = {'value': cap, 'timestamp': now}
                    return cap
        except:
            pass
    if ticker_data and ticker_data.get('quoteVolume', 0) > 0:
        vol = ticker_data['quoteVolume']
        est = min(vol * 20, 100_000_000_000)
        with _market_cap_cache_lock:
            _market_cap_cache[coin] = {'value': est, 'timestamp': now}
        return est
    return 0

# --------------------------- دوال جلب OHLCV (مع Rate Limiter) ---------------------------
def fetch_ohlcv_retry_raw(symbol, timeframe, limit=500, max_retries=4):
    time.sleep(0.1)
    exchange = get_active_exchange()
    last_err = ""
    attempt = 0
    while attempt < max_retries:
        try:
            rest_rate_limiter.wait_if_needed(weight=3)
            actual_limit = limit
            if timeframe == '4h':
                actual_limit = min(limit, 200)
            bars = exchange.fetch_ohlcv(symbol, timeframe, limit=actual_limit)
            if not bars:
                last_err = "بيانات فارغة"
                attempt += 1
                if attempt < max_retries:
                    time.sleep(2**attempt + 4)
                continue
            df = pd.DataFrame(bars, columns=['timestamp','open','high','low','close','volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            for col in ['open','high','low','close','volume']:
                df[col] = df[col].astype('float64')
            return df
        except Exception as e:
            last_err = str(e)
            logger.warning(f"محاولة {attempt+1} لـ {symbol} {timeframe} فشلت: {e}")
            if "418" in str(e) or "banned" in str(e):
                logger.critical("⚠️ تم حظر IP! انتظر 10 دقائق")
                send_telegram("🚨 تم حظر IP من Binance. إيقاف الطلبات لمدة 10 دقائق.")
                try:
                    retry_after = int(exchange.last_response_headers.get('Retry-After', 600))
                except:
                    retry_after = 600
                time.sleep(retry_after + random.randint(0, 30))
            time.sleep(2**attempt + 4)
            attempt += 1
    logger.error(f"فشل جلب {symbol} {timeframe} بعد {max_retries} محاولات: {last_err}")
    return pd.DataFrame()

def fetch_ohlcv_persistent(symbol, timeframe, limit=500, max_attempts=5, retry_interval=10):
    for attempt in range(max_attempts):
        df = fetch_ohlcv_retry_raw(symbol, timeframe, limit, max_retries=1)
        if not df.empty:
            return df
        if attempt < max_attempts - 1:
            logger.info(f"⏳ فشل جلب {symbol} {timeframe}، إعادة محاولة {attempt+1}/{max_attempts} بعد {retry_interval} ثانية...")
            time.sleep(retry_interval)
    return pd.DataFrame()

def fetch_ohlcv_retry(symbol, timeframe, limit=500, max_retries=2, ttl_seconds=60):
    cache_key = f"{symbol}_{timeframe}_{limit}"
    now = time.time()
    with _cache_lock:
        if cache_key in _ohlcv_cache:
            entry = _ohlcv_cache[cache_key]
            if (now - entry['timestamp']) < ttl_seconds:
                return entry['data'].copy()
            else:
                del _ohlcv_cache[cache_key]
    df = fetch_ohlcv_retry_raw(symbol, timeframe, limit, max_retries)
    if not df.empty:
        with _cache_lock:
            _ohlcv_cache[cache_key] = {'data': df, 'timestamp': now}
            if len(_ohlcv_cache) > _ohlcv_cache_max:
                _ohlcv_cache.popitem(last=False)
    return df

def add_advanced_features(df):
    if df.empty or len(df) < 50:
        return df
    df = df.copy()
    for period in [7,9,14,21,25,50,99]:
        df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
    df['sma_50'] = df['close'].rolling(50).mean()
    df['sma_200'] = df['close'].rolling(200).mean()
    bb_period = 20
    bb_std = 2
    df['bb_middle'] = df['close'].rolling(bb_period).mean()
    bb_std_dev = df['close'].rolling(bb_period).std()
    df['bb_upper'] = df['bb_middle'] + bb_std * bb_std_dev
    df['bb_lower'] = df['bb_middle'] - bb_std * bb_std_dev
    df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle'].replace(0, np.nan)
    delta = df['close'].diff()
    gain = delta.where(delta>0,0).rolling(14).mean()
    loss = (-delta.where(delta<0,0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100/(1+rs))
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_diff'] = ema12 - ema26
    df['macd_signal'] = df['macd_diff'].ewm(span=9, adjust=False).mean()
    df['volume_sma'] = df['volume'].rolling(20).mean().replace(0, np.nan)
    df['volume_ratio'] = df['volume'] / df['volume_sma'].replace(0, np.nan)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(1)
    df['atr'] = tr.rolling(14).mean()
    df['atr_percent'] = df['atr'] / df['close'].replace(0, np.nan)
    rsi_min = df['rsi'].rolling(14).min()
    rsi_max = df['rsi'].rolling(14).max()
    rsi_range = (rsi_max - rsi_min).replace(0, np.nan)
    df['stoch_rsi_k'] = np.where(rsi_range == 0, 50.0, (df['rsi'] - rsi_min) / rsi_range * 100)
    df['stoch_rsi_d'] = df['stoch_rsi_k'].rolling(3).mean()
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    money_flow = typical_price * df['volume']
    positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    money_ratio = positive_flow / negative_flow.replace(0, np.nan)
    df['mfi'] = 100 - (100/(1+money_ratio))
    high_diff = df['high'].diff()
    low_diff = -df['low'].diff()
    plus_dm = high_diff.where((high_diff>low_diff)&(high_diff>0),0)
    minus_dm = low_diff.where((low_diff>high_diff)&(low_diff>0),0)
    atr14 = df['atr'].rolling(14).mean().replace(0, np.nan)
    plus_di = 100 * (plus_dm.rolling(14).mean() / atr14)
    minus_di = 100 * (minus_dm.rolling(14).mean() / atr14)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    df['adx'] = dx.rolling(14).mean()
    
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
    df['obv'] = 0.0
    for i in range(1, len(df)):
        if df['close'].iloc[i] > df['close'].iloc[i-1]:
            df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] + df['volume'].iloc[i]
        elif df['close'].iloc[i] < df['close'].iloc[i-1]:
            df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] - df['volume'].iloc[i]
        else:
            df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1]
    df['obv_ma'] = df['obv'].rolling(5).mean()
    df['obv_trend'] = df['obv'] - df['obv_ma']
    
    df = df.replace([np.inf,-np.inf], np.nan)
    df = df.ffill().bfill().fillna(0)
    return df.astype('float64')

def get_cached_features(symbol, timeframe, limit=500, ttl=60):
    cache_key = f"{symbol}_{timeframe}_{limit}"
    now = time.time()
    with _features_cache_lock:
        if cache_key in _features_cache:
            entry = _features_cache[cache_key]
            if (now - entry['timestamp']) < ttl:
                return entry['data'].copy(deep=True) if not entry['data'].empty else pd.DataFrame()
            else:
                del _features_cache[cache_key]
    df = fetch_ohlcv_retry(symbol, timeframe, limit=limit)
    if df.empty:
        return pd.DataFrame()
    if len(df) >= 20:
        df_f = add_advanced_features(df)
    else:
        df_f = df
    if df_f.empty:
        return pd.DataFrame()
    with _features_cache_lock:
        _features_cache[cache_key] = {'data': df_f.copy(deep=True), 'timestamp': now}
        if len(_features_cache) > _features_cache_max:
            _features_cache.popitem(last=False)
    return df_f.copy(deep=True)

# --------------------------- دوال المصادقة ---------------------------
@contextmanager
def acquire_timeout(lock, timeout):
    result = lock.acquire(timeout=timeout)
    if not result:
        raise TimeoutError("فشل الحصول على القفل")
    try:
        yield
    finally:
        lock.release()

def log_security_event(event, details=""):
    try:
        with open(SECURITY_LOG, 'a') as f:
            f.write(f"{datetime.now().isoformat()} | {event} | {details}\n")
    except Exception as e:
        logger.warning(f"فشل تسجيل حدث أمني: {e}")

def save_telegram_last_id():
    global _last_telegram_update_id
    try:
        with open(TELEGRAM_LAST_ID_FILE, 'w') as f:
            f.write(str(_last_telegram_update_id))
    except Exception as e:
        logger.warning(f"فشل حفظ آخر معرف تلغرام: {e}")

def load_telegram_last_id():
    global _last_telegram_update_id
    try:
        if os.path.exists(TELEGRAM_LAST_ID_FILE):
            with open(TELEGRAM_LAST_ID_FILE, 'r') as f:
                _last_telegram_update_id = int(f.read().strip())
    except Exception as e:
        logger.warning(f"فشل تحميل آخر معرف تلغرام: {e}")

def get_pending_exposure_estimate():
    total = 0.0
    with _global_state_lock:
        all_pending = _local_pending_symbols.union(_exchange_pending_symbols)
        if not all_pending:
            return 0.0
        position_size_usdt = bot_stats.last_balance * POSITION_SIZE_PERCENT
        total = len(all_pending) * position_size_usdt
    return total

def _log_stuck_position(symbol, pos, error_msg):
    try:
        with open(STUCK_POSITIONS_LOG, 'a') as f:
            f.write(f"{datetime.now().isoformat()} | {symbol} | {pos.remaining_size} | {pos.entry_price} | {error_msg}\n")
    except Exception as e:
        logger.warning(f"فشل تسجيل المركز العالق: {e}")

# --------------------------- دوال المصادقة Flask ---------------------------
def require_auth(f):
    @wraps(f)
    @limiter.limit("5 per minute", key_func=lambda: request.authorization.username if request.authorization else request.remote_addr)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != ADMIN_USERNAME or auth.password != ADMIN_PASSWORD:
            log_security_event("فشل المصادقة", f"IP: {request.remote_addr}")
            time.sleep(2)
            return ('غير مصرح', 401, {'WWW-Authenticate': 'Basic realm="تسجيل الدخول مطلوب"'})
        return f(*args, **kwargs)
    return decorated

# --------------------------- قالب Dashboard ---------------------------
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><title>بوت v29.0 المبسط</title></head>
<body style="background:#0a0e27;color:#e0e0e0;font-family:sans-serif;padding:20px;">
<h1>🤖 بوت التداول v29.0 (نسخة مبسطة)</h1>
<div id="stats"></div>
<div style="margin-top:20px;">
<button onclick="postAction('/analyze')">🔄 تحليل</button>
<button onclick="postAction('/pause')">⏸️ إيقاف</button>
<button onclick="postAction('/resume')">▶️ استئناف</button>
</div>
<script>
const csrfToken='{{ csrf_token }}';
async function postAction(url){
    await fetch(url,{method:'POST',headers:{'X-CSRFToken':csrfToken}});
    location.reload();
}
fetch('/api/stats').then(r=>r.json()).then(d=>{
    document.getElementById('stats').innerHTML=`<p>الرصيد الكلي: $${d.total_equity.toFixed(2)} | الصفقات المفتوحة: ${d.open_positions} | متوقف: ${d.paused}</p>`;
});
</script>
</body>
</html>
'''

# --------------------------- Routes للإيقاظ والتحكم ---------------------------
@app.route('/ping')
@limiter.exempt
def ping():
    return "pong", 200

@app.route('/')
@require_auth
def dashboard():
    csrf_token = generate_csrf()
    return render_template_string(DASHBOARD_HTML, csrf_token=csrf_token)

@app.route('/api/stats')
@require_auth
def api_stats():
    with _global_state_lock:
        return jsonify({
            'total_equity': get_total_equity(),
            'open_positions': len(open_positions),
            'paused': _is_paused(),
            'daily_trades': _daily_trades_count,
            'daily_loss': _daily_loss_tracker,
            'total_pnl': bot_stats.total_pnl_usdt,
            'stop_loss_25': STOP_LOSS_PARTIAL_1_PERCENT,
            'stop_loss_33': STOP_LOSS_PARTIAL_2_PERCENT,
            'stop_loss_100': STOP_LOSS_FULL_PERCENT
        })

@app.route('/analyze', methods=['POST'])
@require_auth
def force_analyze():
    threading.Thread(target=analyze_and_trade, daemon=True).start()
    return jsonify({'status': 'analyze triggered'})

@app.route('/pause', methods=['POST'])
@require_auth
def pause_bot():
    _set_paused(True)
    save_state()
    return jsonify({'status': 'paused'})

@app.route('/resume', methods=['POST'])
@require_auth
def resume_bot():
    manual_resume()
    return jsonify({'status': 'resumed'})

# --------------------------- دوال التصنيف والظروف ---------------------------
def classify_symbol(symbol, df=None, ticker_data=None):
    if symbol in BASE_SYMBOLS_SET:
        return 'normal'
    if ticker_data:
        current_price = ticker_data.get('last', 0) or 0
        volume_24h = ticker_data.get('quoteVolume', 0) or 0
    else:
        ticker = fetch_ticker_with_retry(symbol)
        current_price = ticker.get('last',0) if ticker else 0
        volume_24h = ticker.get('quoteVolume',0) if ticker else 0
    if current_price < 1.0:
        if volume_24h > 2_000_000:
            return 'normal'
        cap = get_market_cap_from_coingecko(symbol, ticker_data)
        if cap > 50_000_000:
            return 'normal'
        return 'meme'
    if df is None:
        df = fetch_ohlcv_retry(symbol, '1h', limit=200)
    if df.empty or len(df) < 24:
        return 'normal'
    df_f = add_advanced_features(df)
    if df_f.empty or 'atr_percent' not in df_f.columns:
        return 'normal'
    atr_pct = df_f['atr_percent'].mean() if 'atr_percent' in df_f else 0.02
    change = (df['close'].iloc[-1] / df['close'].iloc[-24] - 1) * 100 if len(df) >= 24 else 0
    return 'meme' if (atr_pct > VOLATILITY_THRESHOLD or abs(change) > MEME_PRICE_CHANGE_24H) else 'normal'

def detect_market_regime(symbol):
    df = fetch_ohlcv_retry(symbol, '1h', limit=200)
    if df.empty:
        return 'neutral'
    df_f = get_cached_features(symbol, '1h', limit=200, ttl=120)
    if df_f.empty:
        return 'neutral'
    price = df_f['close'].iloc[-1]
    sma50 = price
    if 'sma_50' in df_f.columns and not pd.isna(df_f['sma_50'].iloc[-1]):
        sma50 = df_f['sma_50'].iloc[-1]
    elif 'ema_50' in df_f.columns and not pd.isna(df_f['ema_50'].iloc[-1]):
        sma50 = df_f['ema_50'].iloc[-1]
    sma200 = price
    if 'sma_200' in df_f.columns and not pd.isna(df_f['sma_200'].iloc[-1]):
        sma200 = df_f['sma_200'].iloc[-1]
    elif 'ema_200' in df_f.columns and not pd.isna(df_f['ema_200'].iloc[-1]):
        sma200 = df_f['ema_200'].iloc[-1]
    adx = df_f['adx'].dropna().iloc[-1] if 'adx' in df_f and len(df_f['adx'].dropna())>0 else 25
    if price < sma50 and sma50 < sma200 and adx > 25:
        return 'trending_down'
    if price > sma50 and sma50 > sma200:
        return 'trending_up'
    if adx > 25:
        return 'trending'
    if 'atr_percent' in df_f.columns:
        if df_f['atr_percent'].iloc[-1] > 0.05:
            return 'high_volatility'
    return 'neutral'

def dynamic_stop_loss_take_profit(entry, atr, side, regime, sym_type, max_sl=None):
    if entry<=0 or atr<=0:
        return entry*0.95, entry*1.05
    mult = STOP_LOSS_MULTIPLIER_MEME if sym_type=='meme' else STOP_LOSS_MULTIPLIER_NORMAL
    if regime=='high_volatility':
        mult *= 1.5
    sl_dist = atr * mult
    tp_dist = atr * 3.0
    if side=='buy':
        sl = entry - sl_dist
        if max_sl and sl_dist > entry*max_sl:
            sl = entry * (1 - max_sl)
        sl = max(sl, entry * 0.01)
        return sl, entry + tp_dist
    else:
        sl = entry + sl_dist
        if max_sl and sl_dist > entry*max_sl:
            sl = entry * (1 + max_sl)
        return sl, entry - tp_dist

def get_scalp_config(sym_type):
    if sym_type=='meme':
        return {'take_profit_levels':TAKE_PROFIT_LEVELS_MEME,'take_profit_percents':TAKE_PROFIT_PERCENTS_MEME,
                'trailing_activation':TRAILING_ACTIVATION_MEME,'trailing_distance':TRAILING_DISTANCE_MEME,
                'timeframe':SCALP_TIMEFRAME,'min_profit':SCALP_MIN_PROFIT,
                'momentum_decay_threshold':MEME_MOMENTUM_DECAY_THRESHOLD,
                'momentum_check_minutes':MEME_MOMENTUM_CHECK_MINUTES,
                'max_no_profit_minutes':MEME_MAX_NO_PROFIT_HOLD_MINUTES}
    else:
        return {'take_profit_levels':TAKE_PROFIT_LEVELS_NORMAL,'take_profit_percents':TAKE_PROFIT_PERCENTS_NORMAL,
                'trailing_activation':TRAILING_ACTIVATION_NORMAL,'trailing_distance':TRAILING_DISTANCE_NORMAL,
                'timeframe':TIMEFRAMES['primary'],'min_profit':STRENGTH_THRESHOLD,
                'momentum_decay_threshold':NORMAL_MOMENTUM_DECAY_THRESHOLD,
                'momentum_check_minutes':NORMAL_MOMENTUM_CHECK_MINUTES,
                'max_no_profit_minutes':NORMAL_MAX_NO_PROFIT_HOLD_MINUTES}

_amount_limits_cache = {}
_amount_limits_cache_time = {}
def get_amount_limits(symbol, max_age=3600):
    now = time.time()
    if symbol in _amount_limits_cache and (now - _amount_limits_cache_time.get(symbol,0)) < max_age:
        return _amount_limits_cache[symbol]
    exchange = get_active_exchange()
    for attempt in range(2):
        try:
            market = exchange.market(symbol)
            min_amt = market['limits']['amount']['min']
            max_amt = market['limits']['amount'].get('max')
            min_cost = market['limits']['cost']['min'] if market['limits']['cost'].get('min') else None
            min_amt = min_amt if min_amt and min_amt>0 else 1.0
            max_amt = max_amt if max_amt and max_amt>0 else None
            min_cost = min_cost if min_cost and min_cost>0 else None
            _amount_limits_cache[symbol] = (min_amt, max_amt, min_cost)
            _amount_limits_cache_time[symbol] = now
            return min_amt, max_amt, min_cost
        except Exception as e:
            logger.warning(f"محاولة {attempt+1} لجلب حدود الكمية لـ {symbol} فشلت: {e}")
            time.sleep(0.5)
    _amount_limits_cache[symbol] = (1.0, None, None)
    return 1.0, None, None

# --------------------------- دوال تنفيذ الأوامر (معدلة) ---------------------------
def execute_limit_order(symbol, side, size, price_ref, sym_type='normal'):
    timeout = LIMIT_TO_MARKET_TIMEOUT
    if _should_simulate():
        return True, price_ref, size, "sim"
    min_amt, max_amt, min_cost = get_amount_limits(symbol)
    if size < min_amt:
        return False, None, 0, None
    if max_amt and size > max_amt:
        size = max_amt
    if min_cost and size * price_ref < min_cost:
        return False, None, 0, None
    exchange = get_active_exchange()
    try:
        size = float(exchange.amount_to_precision(symbol, size))
        price_ref = float(exchange.price_to_precision(symbol, price_ref))
    except Exception as e:
        logger.warning(f"خطأ في تحويل الدقة لـ {symbol}: {e}")

    if side == 'buy':
        adjusted_price = price_ref * (1 + LIMIT_ORDER_SLIPPAGE)
    else:
        adjusted_price = price_ref * (1 - LIMIT_ORDER_SLIPPAGE)
    adjusted_price = float(exchange.price_to_precision(symbol, adjusted_price))

    for attempt in range(2):
        try:
            rest_rate_limiter.wait_if_needed(weight=5)
            params = {}
            order = exchange.create_limit_order(symbol, side, size, adjusted_price, params)
            oid = order['id']
            start = time.time()
            filled = 0.0
            avg_price = 0.0
            while time.time() - start < timeout:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    status = exchange.fetch_order(oid, symbol)
                    filled = float(status.get('filled', 0))
                    if filled > 0:
                        avg_price = float(status.get('average', 0)) or status.get('price', adjusted_price)
                    if status['status'] == 'closed':
                        return True, avg_price, filled, oid
                except Exception:
                    pass
                time.sleep(ORDER_POLL_INTERVAL)
            if filled > 0:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                remaining = size - filled
                if remaining > 0 and MARKET_ORDER_FALLBACK:
                    rest_rate_limiter.wait_if_needed(weight=5)
                    market_order = exchange.create_market_order(symbol, side, remaining)
                    market_filled = float(market_order.get('filled', remaining))
                    market_avg = float(market_order.get('average', 0)) or market_order.get('price', adjusted_price)
                    total_filled = filled + market_filled
                    total_value = (filled * avg_price) + (market_filled * market_avg)
                    final_avg = total_value / total_filled if total_filled > 0 else avg_price
                    return True, final_avg, total_filled, oid
                else:
                    return True, avg_price, filled, oid
            else:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                if MARKET_ORDER_FALLBACK:
                    rest_rate_limiter.wait_if_needed(weight=5)
                    market_order = exchange.create_market_order(symbol, side, size)
                    market_filled = float(market_order.get('filled', size))
                    market_avg = float(market_order.get('average', 0)) or market_order.get('price', adjusted_price)
                    return True, market_avg, market_filled, market_order.get('id')
                else:
                    return False, None, 0, None
        except Exception as e:
            if "418" in str(e) or "banned" in str(e):
                logger.critical(f"🚨 تم حظر IP أثناء أمر {symbol}")
                try:
                    retry_after = int(exchange.last_response_headers.get('Retry-After', 600))
                except:
                    retry_after = 600
                time.sleep(retry_after + random.randint(0, 30))
            logger.error(f"خطأ في الأمر {symbol}: {e}")
            generate_error_report("فشل_أمر", "أوامر", f"فشل limit order {symbol}: {e}")
            if attempt == 1:
                return False, None, 0, None
            continue
    return False, None, 0, None

def execute_limit_close(symbol, side, size, price_ref, sym_type='normal', extra_attempt=False):
    timeout = LIMIT_TO_MARKET_TIMEOUT
    if _should_simulate():
        return True, price_ref, size, "sim"
    min_amt, max_amt, min_cost = get_amount_limits(symbol)
    if size < min_amt:
        return False, None, 0, None
    if max_amt and size > max_amt:
        size = max_amt
    exchange = get_active_exchange()
    try:
        size = float(exchange.amount_to_precision(symbol, size))
        price_ref = float(exchange.price_to_precision(symbol, price_ref))
    except Exception as e:
        logger.warning(f"خطأ في تحويل الدقة لـ {symbol}: {e}")

    if side == 'sell':
        adjusted_price = price_ref * (1 - LIMIT_ORDER_SLIPPAGE)
    else:
        adjusted_price = price_ref * (1 + LIMIT_ORDER_SLIPPAGE)
    adjusted_price = float(exchange.price_to_precision(symbol, adjusted_price))

    for attempt in range(2 if extra_attempt else 1):
        try:
            rest_rate_limiter.wait_if_needed(weight=5)
            order = exchange.create_limit_order(symbol, side, size, adjusted_price)
            oid = order['id']
            start = time.time()
            filled = 0.0
            avg_price = 0.0
            while time.time() - start < timeout:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    status = exchange.fetch_order(oid, symbol)
                    filled = float(status.get('filled', 0))
                    if filled > 0:
                        avg_price = float(status.get('average', 0)) or status.get('price', adjusted_price)
                    if status['status'] == 'closed':
                        return True, avg_price, filled, oid
                except Exception:
                    pass
                time.sleep(ORDER_POLL_INTERVAL)
            if filled > 0:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                remaining = size - filled
                if remaining > 0 and MARKET_ORDER_FALLBACK:
                    rest_rate_limiter.wait_if_needed(weight=5)
                    market_order = exchange.create_market_order(symbol, side, remaining)
                    market_filled = float(market_order.get('filled', remaining))
                    market_avg = float(market_order.get('average', 0)) or market_order.get('price', adjusted_price)
                    total_filled = filled + market_filled
                    total_value = (filled * avg_price) + (market_filled * market_avg)
                    final_avg = total_value / total_filled if total_filled > 0 else avg_price
                    return True, final_avg, total_filled, oid
                else:
                    return True, avg_price, filled, oid
            else:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                if MARKET_ORDER_FALLBACK:
                    rest_rate_limiter.wait_if_needed(weight=5)
                    market_order = exchange.create_market_order(symbol, side, size)
                    market_filled = float(market_order.get('filled', size))
                    market_avg = float(market_order.get('average', 0)) or market_order.get('price', adjusted_price)
                    return True, market_avg, market_filled, market_order.get('id')
                else:
                    return False, None, 0, None
        except Exception as e:
            logger.error(f"فشل close limit {symbol}: {e}")
            if not extra_attempt or attempt == 1:
                generate_error_report("فشل_أمر", "أوامر", f"فشل close limit {symbol}: {e}")
                return False, None, 0, None
    return False, None, 0, None

def validate_restored_position(symbol, pos):
    if TEST_MODE or PAPER_TRADING or not ENABLE_TRADING:
        return True
    for attempt in range(3):
        try:
            exchange = get_active_exchange()
            rest_rate_limiter.wait_if_needed(weight=20)
            balance = exchange.fetch_balance()
            coin = symbol.split('/')[0]
            total = balance.get(coin, {}).get('total', 0)
            if total < pos.remaining_size * 0.01:
                logger.warning(f"⚠️ المركز {symbol} لم يعد موجوداً في المنصة. سيتم حذفه.")
                send_telegram(f"ℹ️ تم حذف المركز {symbol} تلقائياً (غير موجود في المحفظة).")
                return False
            return True
        except Exception as e:
            logger.warning(f"خطأ في التحقق من المركز {symbol} (محاولة {attempt+1}/3): {e}")
            time.sleep(1)
    logger.error(f"فشل التحقق من المركز {symbol} بعد 3 محاولات، سيتم الاحتفاظ به افتراضياً")
    return True

# ======================================================================
# ======================= النماذج المطورة (لجنة الشراء) ===============
# ======================================================================

class AdvancedRuleModel:
    def __init__(self):
        pass
    def get_signal_percent(self, df):
        if len(df) < 20:
            return 0.5, 0.5
        try:
            close = df['close'].values
            open_p = df['open'].values
            high = df['high'].values
            low = df['low'].values
            volume = df['volume'].values
            volume_sma = df['volume_sma'].values if 'volume_sma' in df else None
            rsi = df['rsi'].values if 'rsi' in df else None
            n = min(10, len(df))
            last_close = close[-1]
            last_open = open_p[-1]
            last_high = high[-1]
            last_low = low[-1]
            last_volume = volume[-1]

            accumulation_score = 0.0
            green_count = 0
            avg_body_ratio = 0.0
            for i in range(4, 0, -1):
                idx = -i
                if close[idx] > open_p[idx]:
                    green_count += 1
                    body = abs(close[idx] - open_p[idx])
                    range_p = high[idx] - low[idx] if high[idx] - low[idx] > 0 else 1e-6
                    body_ratio = body / range_p
                    avg_body_ratio += body_ratio
            if green_count >= 3:
                avg_body_ratio /= green_count
                if avg_body_ratio < 0.5:
                    accumulation_score = 0.6 + (green_count - 3) * 0.1
                    if close[-1] > open_p[-1] and close[-1] > high[-2]:
                        accumulation_score += 0.2
                elif avg_body_ratio < 0.7:
                    accumulation_score = 0.4 + (green_count - 3) * 0.1
            accumulation_score = min(1.0, accumulation_score)

            breakout_score = 0.0
            resistance = max(high[-11:-1]) if len(high) >= 11 else max(high[:-1])
            if last_close > resistance:
                body = abs(last_close - last_open)
                range_p = last_high - last_low if last_high - last_low > 0 else 1e-6
                body_ratio = body / range_p
                if body_ratio > 0.6 and last_close > last_open:
                    breakout_score = 0.9
                    if volume_sma is not None and last_volume > volume_sma[-1] * 1.3:
                        breakout_score = min(1.0, breakout_score + 0.1)
                elif body_ratio > 0.4:
                    breakout_score = 0.6
                else:
                    breakout_score = 0.4
            elif last_close > resistance * 0.995:
                breakout_score = 0.3

            reversal_score = 0.0
            if len(high) >= 3:
                lower_shadow = min(last_open, last_close) - last_low
                upper_shadow = last_high - max(last_open, last_close)
                total_range = last_high - last_low if last_high - last_low > 0 else 1e-6
                lower_ratio = lower_shadow / total_range
                body_ratio = abs(last_close - last_open) / total_range
                recent_down = sum(1 for i in range(3) if close[-1-i] < open_p[-1-i]) >= 2
                if recent_down and lower_ratio > 0.6 and body_ratio < 0.3:
                    reversal_score += 0.7
            if len(close) >= 2:
                prev_close = close[-2]
                prev_open = open_p[-2]
                if prev_close < prev_open:
                    if last_close > last_open and last_close > prev_open and last_open < prev_close:
                        reversal_score += 0.8
            if len(close) >= 3:
                soldiers = True
                for i in range(3):
                    idx = -1 - i
                    if not (close[idx] > open_p[idx] and close[idx] > close[idx-1]):
                        soldiers = False
                        break
                if soldiers:
                    bodies = [abs(close[-1-j] - open_p[-1-j]) for j in range(3)]
                    if bodies[0] < bodies[1] < bodies[2]:
                        reversal_score = min(1.0, reversal_score + 0.9)

            momentum_score = 0.0
            if len(close) >= 6:
                mom_5 = (close[-1] - close[-6]) / (close[-6] + 1e-6)
                if mom_5 > 0.02:
                    momentum_score = 0.8 + min(0.2, mom_5 * 2)
                elif mom_5 > 0.01:
                    momentum_score = 0.6
                elif mom_5 > 0.005:
                    momentum_score = 0.4
                else:
                    momentum_score = 0.2
                if volume_sma is not None and last_volume > volume_sma[-1] * 1.5:
                    momentum_score = min(1.0, momentum_score + 0.1)

            buy_score = (accumulation_score * 0.25 +
                         breakout_score * 0.35 +
                         reversal_score * 0.20 +
                         momentum_score * 0.20)
            buy_score = max(0.0, min(1.0, buy_score))
            sell_score = max(0.0, min(1.0, 1.0 - buy_score + 0.05))
            return buy_score, sell_score
        except Exception as e:
            return 0.5, 0.5

class AdvancedMomentumFlowModel:
    def __init__(self):
        pass
    def update(self, symbol, df):
        if len(df) < 30:
            return {'buy': 0.5, 'sell': 0.5}
        try:
            close = df['close'].values
            rsi = df['rsi'].values if 'rsi' in df else np.full(len(df), 50)
            vol_ratio = df['volume_ratio'].values if 'volume_ratio' in df else np.ones(len(df))
            bb_upper = df['bb_upper'].values if 'bb_upper' in df else None
            bb_lower = df['bb_lower'].values if 'bb_lower' in df else None
            bb_middle = df['bb_middle'].values if 'bb_middle' in df else None

            last_rsi = rsi[-1]
            prev_rsi = rsi[-2] if len(rsi) > 1 else 50
            last_vol_ratio = vol_ratio[-1]
            last_close = close[-1]
            prev_close = close[-2] if len(close) > 1 else last_close

            overbought_score = 0.0
            oversold_score = 0.0
            if last_rsi > 75:
                overbought_score = 1.0
            elif last_rsi > 65:
                overbought_score = 0.7
            elif last_rsi > 55:
                overbought_score = 0.4
            else:
                overbought_score = 0.1
            if last_rsi < 25:
                oversold_score = 1.0
            elif last_rsi < 35:
                oversold_score = 0.7
            elif last_rsi < 45:
                oversold_score = 0.4
            else:
                oversold_score = 0.1

            if bb_upper is not None and bb_lower is not None and bb_middle is not None:
                bb_width = (bb_upper[-1] - bb_lower[-1]) / (bb_middle[-1] if bb_middle[-1] != 0 else 1e-6)
                if bb_width > 0.05 and last_rsi > 70:
                    overbought_score = min(1.0, overbought_score + 0.2)
                elif bb_width < 0.02 and last_rsi < 30:
                    oversold_score = min(1.0, oversold_score + 0.2)

            divergence_penalty = 0.0
            if len(close) >= 5:
                price_change = (close[-1] - close[-5]) / (close[-5] + 1e-6)
                rsi_change = rsi[-1] - rsi[-5]
                if price_change > 0.02 and rsi_change < -3:
                    divergence_penalty = 0.6
                elif price_change > 0.01 and rsi_change < -1:
                    divergence_penalty = 0.3

            volume_weakness = 0.0
            if last_vol_ratio < 0.8 and last_close > prev_close:
                volume_weakness = 0.5
            elif last_vol_ratio < 0.6 and last_close > prev_close:
                volume_weakness = 0.8
            elif last_vol_ratio > 1.5 and last_close > prev_close:
                volume_weakness = -0.3

            fear_score = (overbought_score * 0.40 +
                         divergence_penalty * 0.30 +
                         max(0, volume_weakness) * 0.20 +
                         (1 - oversold_score) * 0.10)
            fear_score = max(0.0, min(1.0, fear_score))
            buy_score = 1.0 - fear_score
            if oversold_score > 0.7:
                buy_score = min(1.0, buy_score + 0.2)
            if divergence_penalty > 0.5:
                buy_score = max(0.0, buy_score - 0.3)
            buy_score = max(0.0, min(1.0, buy_score))
            sell_score = max(0.0, min(1.0, 1.0 - buy_score + 0.05))
            return {'buy': buy_score, 'sell': sell_score}
        except Exception:
            return {'buy': 0.5, 'sell': 0.5}

class VWAP_OBV_Model:
    def get_score(self, df):
        if len(df) < 15:
            return 0.5
        try:
            close = df['close'].values
            vwap = df['vwap'].values if 'vwap' in df else None
            obv = df['obv'].values if 'obv' in df else None
            volume_ratio = df['volume_ratio'].values if 'volume_ratio' in df else np.ones(len(df))
            if vwap is None or obv is None:
                return 0.5
            last_close = close[-1]
            last_vwap = vwap[-1]
            last_obv = obv[-1]
            last_vol_ratio = volume_ratio[-1]
            if last_vwap > 0:
                vwap_dev = (last_close - last_vwap) / last_vwap
            else:
                vwap_dev = 0.0
            if vwap_dev > 0.05:
                vwap_score = 0.1
            elif vwap_dev > 0.03:
                vwap_score = 0.3
            elif vwap_dev > 0.01:
                vwap_score = 0.6
            elif vwap_dev > -0.01:
                vwap_score = 0.9
            elif vwap_dev > -0.03:
                vwap_score = 0.7
            else:
                vwap_score = 0.4
            if len(obv) >= 6:
                obv_slope = (obv[-1] - obv[-6]) / (abs(obv[-6]) + 1e-6)
                if obv_slope > 0.05:
                    obv_score = 0.9
                elif obv_slope > 0.02:
                    obv_score = 0.7
                elif obv_slope > 0.0:
                    obv_score = 0.5
                elif obv_slope > -0.02:
                    obv_score = 0.3
                else:
                    obv_score = 0.1
            else:
                obv_score = 0.5
            volume_breakout = 0.0
            if last_vol_ratio > 1.5 and vwap_dev > 0.01:
                volume_breakout = 0.8
            elif last_vol_ratio > 1.2 and vwap_dev > 0.005:
                volume_breakout = 0.6
            elif last_vol_ratio > 1.5 and vwap_dev < -0.01:
                volume_breakout = 0.2
            else:
                volume_breakout = 0.3
            buy_score = (vwap_score * 0.45 +
                         obv_score * 0.35 +
                         volume_breakout * 0.20)
            if vwap_dev > 0.05 and obv_score < 0.4:
                buy_score = max(0.0, buy_score - 0.3)
            if abs(vwap_dev) < 0.01 and obv_score > 0.6:
                buy_score = min(1.0, buy_score + 0.2)
            buy_score = max(0.0, min(1.0, buy_score))
            return buy_score
        except Exception:
            return 0.5

class CFHMModel:
    def __init__(self, flow_win=14, mom_win=5):
        self.flow_win = flow_win
        self.mom_win = mom_win
    def calculate_scores(self, df):
        if len(df) < max(self.flow_win, self.mom_win): return None, None
        raw_flow = ((df['close']-df['low']) - (df['high']-df['close'])) * df['volume']
        avg_flow = raw_flow.rolling(self.flow_win).mean()
        cur_flow = avg_flow.iloc[-1] if not np.isnan(avg_flow.iloc[-1]) else 0.0
        abs_flow = (raw_flow.abs().rolling(self.flow_win).mean()).iloc[-1]
        flow_score = max(0.0, min(1.0, cur_flow/abs_flow+0.5)) if abs_flow>0 else 0.5
        returns = df['close'].pct_change(self.mom_win)
        ret_std = returns.rolling(self.mom_win).std().iloc[-1]
        high_low = df['high']-df['low']
        high_close = (df['high']-df['close'].shift()).abs()
        low_close = (df['low']-df['close'].shift()).abs()
        tr = pd.concat([high_low,high_close,low_close], axis=1).max(1)
        atr = tr.rolling(14).mean().iloc[-1]
        if atr>0: hidden = ret_std / (atr/df['close'].iloc[-1])
        else: hidden=0.0
        mom_score = min(1.0, hidden*1.5)
        vol_sma = df['volume'].rolling(20).mean().iloc[-1]
        vol_ratio = df['volume'].iloc[-1] / vol_sma if vol_sma>0 else 1.0
        vol_score = max(0.0, min(1.0, (min(2.0,vol_ratio)-0.8)/1.2))
        score = 0.5*flow_score + 0.3*mom_score + 0.2*vol_score
        buy_percent = max(0.0, min(1.0, score))
        sell_percent = max(0.0, min(1.0, 1.0 - score))
        return buy_percent, sell_percent

class EntryTimingModel:
    def get_entry_score(self, df):
        if len(df) < 20:
            return 0.5
        recent_high = df['high'].iloc[-6:].max()
        recent_low = df['low'].iloc[-6:].min()
        current_price = df['close'].iloc[-1]
        price_range = recent_high - recent_low
        if price_range == 0:
            return 0.5
        position_in_range = (current_price - recent_low) / price_range
        last_candle_change = (df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] if len(df) >= 2 else 0
        momentum_5 = (df['close'].iloc[-1] - df['close'].iloc[-6]) / df['close'].iloc[-6] if len(df) >= 6 else 0
        score = 0.5
        if 0.3 < position_in_range < 0.7 and momentum_5 > 0.005 and momentum_5 < 0.03:
            score = 0.9
        elif position_in_range < 0.4 and last_candle_change > 0.002:
            score = 0.8
        elif 0.2 < position_in_range < 0.8:
            score = 0.6
        elif position_in_range > 0.95:
            score = 0.2
        elif momentum_5 > 0.04:
            score = 0.3
        elif position_in_range > 0.85 and momentum_5 < 0.01:
            score = 0.1
        if score >= 0.99:
            return 0.1
        elif score >= 0.85:
            return 0.2
        else:
            return score

# --------------------------- دالة مساعدة لحساب المضاعف ---------------------------
def get_multiplier_from_score(score_5m, score_15m=None):
    if score_15m is None:
        if score_5m < 0.35:    return 0.75
        elif score_5m < 0.40:  return 0.80
        elif score_5m < 0.45:  return 0.85
        elif score_5m < 0.50:  return 0.90
        elif score_5m < 0.55:  return 1.00
        elif score_5m < 0.60:  return 1.05
        elif score_5m < 0.65:  return 1.10
        elif score_5m < 0.75:  return 1.15
        elif score_5m < 0.80:  return 1.20
        else:                  return 1.25
    ratio = score_5m / (score_15m + 1e-6)
    if ratio > 1.30:    return 0.70
    elif ratio > 1.15:  return 0.80
    elif ratio > 1.05:  return 0.90
    elif ratio > 0.95:  return 1.00
    elif ratio > 0.80:  return 1.10
    elif ratio > 0.65:  return 1.20
    else:               return 1.30

# --------------------------- لجنة الشراء ---------------------------
class BuyingCommittee:
    def __init__(self):
        self.models = {
            'rule': AdvancedRuleModel(),
            'emr': AdvancedMomentumFlowModel(),
            'cfhm': CFHMModel(),
            'timing': EntryTimingModel(),
            'vwap_obv': VWAP_OBV_Model()
        }
        self.weights = {
            'rule': 0.0,
            'emr': 0.0,
            'cfhm': 0.0,
            'timing': 1.0,
            'vwap_obv': 0.0
        }
        self.DEFAULT_THRESHOLDS = {
            'normal': {
                'bad': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                        'weak_penalty_15m':0.15, 'strong_bonus_15m':0.10,
                        'weak_penalty_1h':0.15, 'strong_bonus_1h':0.10},
                'normal': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                           'weak_penalty_15m':0.12, 'strong_bonus_15m':0.12,
                           'weak_penalty_1h':0.12, 'strong_bonus_1h':0.12},
                'good': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                         'weak_penalty_15m':0.10, 'strong_bonus_15m':0.15,
                         'weak_penalty_1h':0.10, 'strong_bonus_1h':0.15}
            },
            'meme': {
                'bad': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                        'weak_penalty_15m':0.12, 'strong_bonus_15m':0.08,
                        'weak_penalty_1h':0.12, 'strong_bonus_1h':0.08},
                'normal': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                           'weak_penalty_15m':0.10, 'strong_bonus_15m':0.10,
                           'weak_penalty_1h':0.10, 'strong_bonus_1h':0.10},
                'good': {'min_conf':0.52, 'thresh3':0.60, 'thresh2':0.56, 'thresh1':0.52,
                         'weak_penalty_15m':0.08, 'strong_bonus_15m':0.12,
                         'weak_penalty_1h':0.08, 'strong_bonus_1h':0.12}
            }
        }
        self.original_thresholds = None
        self.apply_multiplier(1.0, save=False)

    def apply_multiplier(self, percent_multiplier, save=True):
        if self.original_thresholds is None:
            from copy import deepcopy
            self.original_thresholds = deepcopy(self.DEFAULT_THRESHOLDS)
        self.thresholds = {}
        for sym_type in self.original_thresholds:
            self.thresholds[sym_type] = {}
            for market_cond in self.original_thresholds[sym_type]:
                orig = self.original_thresholds[sym_type][market_cond]
                new = {}
                for key, val in orig.items():
                    if isinstance(val, (int, float)):
                        new[key] = max(0.05, min(0.95, val * percent_multiplier))
                    else:
                        new[key] = val
                self.thresholds[sym_type][market_cond] = new
        global CURRENT_BUY_COMMITTEE_MULTIPLIER
        CURRENT_BUY_COMMITTEE_MULTIPLIER = percent_multiplier
        if save:
            save_state()
        logger.info(f"تم تغيير عتبات لجنة الشراء بمضاعف {percent_multiplier:.2f}")

    def get_multiplier(self):
        return CURRENT_BUY_COMMITTEE_MULTIPLIER

    def _get_scores(self, df):
        if df is None or df.empty or len(df) < 20:
            return {model: 0.5 for model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']}
        scores = {}
        rule_buy, _ = self.models['rule'].get_signal_percent(df)
        scores['rule'] = rule_buy if rule_buy is not None else 0.5
        try:
            emr_res = self.models['emr'].update('symbol', df)
            scores['emr'] = emr_res['buy'] if emr_res else 0.5
        except:
            scores['emr'] = 0.5
        cfhm_buy, _ = self.models['cfhm'].calculate_scores(df)
        scores['cfhm'] = cfhm_buy if cfhm_buy is not None else 0.5
        scores['timing'] = self.models['timing'].get_entry_score(df)
        scores['vwap_obv'] = self.models['vwap_obv'].get_score(df)
        return scores

    def calculate_weighted_average(self, df_5m, df_15m, df_1h, symbol, sym_type='normal', market_condition='normal'):
        scores_5m = self._get_scores(df_5m)
        scores_15m = self._get_scores(df_15m)
        scores_1h = self._get_scores(df_1h)
        avg_scores = {}
        for model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
            avg_scores[model] = (
                scores_5m[model] * WEIGHT_5M +
                scores_15m[model] * WEIGHT_15M +
                scores_1h[model] * WEIGHT_1H
            ) / 100.0
        weighted_avg = sum(avg_scores[m] * self.weights[m] for m in self.weights)
        rule_emr_sum = avg_scores['rule'] + avg_scores['emr']
        return {
            'avg_scores': avg_scores,
            'weighted_avg': weighted_avg,
            'rule_emr_sum': rule_emr_sum,
            'scores_5m': scores_5m,
            'scores_15m': scores_15m,
            'scores_1h': scores_1h
        }

    def decide(self, df_5m, df_15m, df_1h, symbol, sym_type='normal', market_condition='normal'):
        global SINGLE_MODEL_FILTER_MODEL, SINGLE_MODEL_FILTER_THRESHOLD, SINGLE_MODEL_FILTER_TIMEFRAME, SINGLE_MODEL_FILTER_ENABLED
        global SECOND_MODEL_FILTER_MODEL, SECOND_MODEL_FILTER_THRESHOLD, SECOND_MODEL_FILTER_TIMEFRAME, SECOND_MODEL_FILTER_ENABLED
        global THIRD_MODEL_FILTER_MODEL, THIRD_MODEL_FILTER_THRESHOLD, THIRD_MODEL_FILTER_TIMEFRAME, THIRD_MODEL_FILTER_ENABLED
        global FOURTH_MODEL_FILTER_MODEL, FOURTH_MODEL_FILTER_THRESHOLD, FOURTH_MODEL_FILTER_TIMEFRAME, FOURTH_MODEL_FILTER_ENABLED
        global FIFTH_MODEL_FILTER_MODEL, FIFTH_MODEL_FILTER_THRESHOLD, FIFTH_MODEL_FILTER_TIMEFRAME, FIFTH_MODEL_FILTER_ENABLED
        global SIXTH_MODEL_FILTER_MODEL, SIXTH_MODEL_FILTER_THRESHOLD, SIXTH_MODEL_FILTER_TIMEFRAME, SIXTH_MODEL_FILTER_ENABLED
        global SEVENTH_MODEL_FILTER_MODEL, SEVENTH_MODEL_FILTER_THRESHOLD, SEVENTH_MODEL_FILTER_TIMEFRAME, SEVENTH_MODEL_FILTER_ENABLED
        
        res = self.calculate_weighted_average(df_5m, df_15m, df_1h, symbol, sym_type, market_condition)
        avg_scores = res['avg_scores']
        avg = res['weighted_avg']
        scores_5m = res['scores_5m']
        scores_15m = res['scores_15m']
        scores_1h = res['scores_1h']

        if SINGLE_MODEL_FILTER_ENABLED:
            if SINGLE_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif SINGLE_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(SINGLE_MODEL_FILTER_MODEL, 0.5)
            if selected_score < SINGLE_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر1 - حد أدنى): {SINGLE_MODEL_FILTER_MODEL} ({SINGLE_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {SINGLE_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if SECOND_MODEL_FILTER_ENABLED:
            if SECOND_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif SECOND_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(SECOND_MODEL_FILTER_MODEL, 0.5)
            if selected_score < SECOND_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر2 - حد أدنى): {SECOND_MODEL_FILTER_MODEL} ({SECOND_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {SECOND_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if THIRD_MODEL_FILTER_ENABLED:
            if THIRD_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif THIRD_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(THIRD_MODEL_FILTER_MODEL, 0.5)
            if selected_score < THIRD_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر3 - حد أدنى): {THIRD_MODEL_FILTER_MODEL} ({THIRD_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {THIRD_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if FOURTH_MODEL_FILTER_ENABLED:
            if FOURTH_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif FOURTH_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(FOURTH_MODEL_FILTER_MODEL, 0.5)
            if selected_score < FOURTH_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر4 - حد أدنى): {FOURTH_MODEL_FILTER_MODEL} ({FOURTH_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {FOURTH_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if FIFTH_MODEL_FILTER_ENABLED:
            if FIFTH_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif FIFTH_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(FIFTH_MODEL_FILTER_MODEL, 0.5)
            if selected_score > FIFTH_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر5 - حد أعلى): {FIFTH_MODEL_FILTER_MODEL} ({FIFTH_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} > {FIFTH_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if SIXTH_MODEL_FILTER_ENABLED:
            if SIXTH_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif SIXTH_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(SIXTH_MODEL_FILTER_MODEL, 0.5)
            if selected_score < SIXTH_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر6 - حد أدنى): {SIXTH_MODEL_FILTER_MODEL} ({SIXTH_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {SIXTH_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        if SEVENTH_MODEL_FILTER_ENABLED:
            if SEVENTH_MODEL_FILTER_TIMEFRAME == '5m':
                scores = scores_5m
            elif SEVENTH_MODEL_FILTER_TIMEFRAME == '1h':
                scores = scores_1h
            else:
                scores = scores_15m
            selected_score = scores.get(SEVENTH_MODEL_FILTER_MODEL, 0.5)
            if selected_score < SEVENTH_MODEL_FILTER_THRESHOLD:
                reason = f"❌ رفض (فلتر7 - حد أدنى): {SEVENTH_MODEL_FILTER_MODEL} ({SEVENTH_MODEL_FILTER_TIMEFRAME}) = {selected_score:.3f} < {SEVENTH_MODEL_FILTER_THRESHOLD:.2f}"
                return 'neutral', 0, 0, reason, avg_scores, scores_5m, scores_15m, scores_1h

        cfg = self.thresholds.get(sym_type, {}).get(market_condition, self.thresholds['normal']['normal'])
        thresh = cfg.get('thresh1', 0.52)
        min_conf = cfg['min_conf']

        if avg > thresh and avg >= min_conf:
            return 'buy', avg, avg, f"✅ موافقة: المتوسط={avg:.3f} | العتبة={thresh:.3f}", avg_scores, scores_5m, scores_15m, scores_1h
        else:
            if avg < min_conf:
                main_reason = f"الثقة منخفضة جداً ({avg:.3f} < {min_conf:.3f})"
            elif avg <= thresh:
                main_reason = f"لم يتجاوز العتبة المطلوبة ({avg:.3f} ≤ {thresh:.3f})"
            else:
                main_reason = "فشل الشروط الإضافية"
            return 'neutral', avg, avg, f"❌ رفض: {main_reason}", avg_scores, scores_5m, scores_15m, scores_1h

buying_committee = BuyingCommittee()

# --------------------------- دالة جلب دفتر الطلبات (للسيولة فقط) ---------------------------
_orderbook_cache = {}
_orderbook_cache_lock = threading.Lock()
_ORDERBOOK_CACHE_MAX = 500
def fetch_orderbook_with_cache(symbol, limit=LIQUIDITY_CHECK_DEPTH, ttl=5):
    now = time.time()
    with _orderbook_cache_lock:
        key = f"{symbol}_{limit}"
        if key in _orderbook_cache:
            entry = _orderbook_cache[key]
            if now - entry['timestamp'] < ttl: return entry['data']
    try:
        rest_rate_limiter.wait_if_needed(weight=5)
        ob = get_active_exchange().fetch_order_book(symbol, limit=limit)
        with _orderbook_cache_lock:
            if len(_orderbook_cache) > _ORDERBOOK_CACHE_MAX:
                items = list(_orderbook_cache.items())
                for old_key, _ in items[:50]: del _orderbook_cache[old_key]
            _orderbook_cache[key] = {'data': ob, 'timestamp': now}
        return ob
    except Exception as e:
        logger.warning(f"فشل جلب الدفتر لـ {symbol}: {e}")
        return None

# --------------------------- دوال التداول ---------------------------
def log_trade(symbol, side, entry, exit, size, pnl, reason, pred, conf, regime, sym_type, ai_approved=False):
    trade_logger.info(f"{symbol},{side},{entry},{exit},{size},{pnl},{reason},{pred},{conf},{regime},{sym_type},{ai_approved}")
    global _daily_biggest_win, _daily_biggest_loss, _daily_most_traded
    with _global_state_lock:
        if pnl > 0:
            if pnl > _daily_biggest_win: _daily_biggest_win = pnl
            bot_stats.weekly_wins += 1
            bot_stats.weekly_pnl += pnl
        else:
            if pnl < _daily_biggest_loss: _daily_biggest_loss = pnl
            bot_stats.weekly_losses += 1
            bot_stats.weekly_pnl += pnl
        _daily_most_traded[symbol] += 1
        bot_stats.symbol_performance[symbol] = bot_stats.symbol_performance.get(symbol, 0) + pnl
    bot_stats.add_equity_point(get_total_equity())
    save_state()

def close_partial(symbol, percent, price, reason, pnl_usdt=None):
    global _daily_loss_tracker, _daily_trades_count, _daily_winning_trades, _daily_losing_trades
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    with _global_state_lock:
        pos = open_positions.get(symbol)
        if not pos: return
        if not validate_restored_position(symbol, pos):
            del open_positions[symbol]
            with _cooldown_lock:
                _symbol_cooldown_until[symbol] = datetime.now() + timedelta(minutes=COOLDOWN_LOSS_MINUTES)
            _local_pending_symbols.discard(symbol)
            _exchange_pending_symbols.discard(symbol)
            save_state()
            send_telegram(f"ℹ️ تم حذف المركز {symbol} تلقائياً (غير موجود في المحفظة)")
            return
        if pos._closing: return
        pos._closing = True
    try:
        if percent < 1.0:
            try:
                _, _, min_cost = get_amount_limits(symbol)
            except Exception as e:
                logger.warning(f"⚠️ فشل جلب min_cost لـ {symbol} في close_partial: {e}")
                min_cost = 5.0
            if min_cost is None:
                min_cost = 5.0
            close_size_original = pos.remaining_size * percent
            part_value = close_size_original * price
            if part_value < min_cost:
                logger.info(f"⚠️ قيمة البيع الجزئي لـ {symbol} = ${part_value:.2f} أقل من min_cost (${min_cost:.2f}). سيتم بيع المركز بالكامل بدلاً من {percent*100:.0f}%.")
                percent = 1.0
                reason = f"{reason} (بيع كامل بدلاً من جزئي بسبب صغر القيمة)"
                close_size = pos.remaining_size
            else:
                close_size = close_size_original
        else:
            close_size = pos.remaining_size

        if close_size <= 0: return
        fill_price = price
        filled_size = close_size
        success = False
        is_sim = _should_simulate()
        pnl_net = None
        try:
            if not is_sim:
                close_side = 'sell' if pos.side=='buy' else 'buy'
                for attempt in range(MAX_RETRIES_CLOSE):
                    ws_data = ws_manager.get_ticker(symbol)
                    if ws_data and ws_data.get('price', 0) > 0:
                        curr = ws_data['price']
                    else:
                        ticker = fetch_ticker_with_retry(symbol, max_retries=1)
                        curr = ticker['last'] if ticker else price
                    success, fill_price, filled_size, _ = execute_limit_close(symbol, close_side, close_size, curr, pos.symbol_type, extra_attempt=(attempt==MAX_RETRIES_CLOSE-1))
                    if success: break
                    time.sleep(0.8*(attempt+1))
                if not success:
                    try:
                        ws_data = ws_manager.get_ticker(symbol)
                        if ws_data and ws_data.get('price', 0) > 0:
                            curr = ws_data['price']
                        else:
                            ticker = fetch_ticker_with_retry(symbol, max_retries=1)
                            curr = ticker['last'] if ticker else price
                        exchange = get_active_exchange()
                        if pos.side=='buy': order = exchange.create_market_sell_order(symbol, close_size)
                        else: order = exchange.create_market_buy_order(symbol, close_size)
                        fill_price = order.get('average', order.get('price', curr))
                        filled_size = order.get('filled', close_size)
                        success = True
                        slippage = abs(fill_price - price) / price
                        max_slippage = MAX_SLIPPAGE_EMERGENCY_MEME if pos.symbol_type == 'meme' else MAX_SLIPPAGE_EMERGENCY_NORMAL
                        if slippage > max_slippage: send_telegram(f"⚠️ انزلاق كبير ({slippage:.2%}) عند إغلاق {symbol}")
                    except Exception as e:
                        if "Insufficient" in str(e):
                            logger.warning(f"⚠️ رصيد غير كافٍ لإغلاق {symbol}، اعتبار المركز مغلقاً")
                            fill_price = price
                            filled_size = close_size
                            success = True
                            pnl_net = 0.0
                        else:
                            with _global_state_lock:
                                pos.last_fail_time = datetime.now()
                                pos.retry_count += 1
                            _log_stuck_position(symbol, pos, str(e))
                            save_state()
                            return
            else: success = True
            if not success:
                with _global_state_lock:
                    pos.last_fail_time = datetime.now()
                    pos.retry_count += 1
                _log_stuck_position(symbol, pos, "فشل كل محاولات الإغلاق")
                save_state()
                return
            if pnl_net is None:
                pnl_net = calculate_net_pnl(symbol, pos.entry_price, fill_price, filled_size, pos.side, pos.symbol_type)
            if not is_sim:
                new_balance = _safe_fetch_balance_after_trade(attempts=10, delay=3.0, silent=True)
                if new_balance is not None:
                    with _global_state_lock: bot_stats.last_balance = new_balance
                else: logger.error(f"⚠️ فشل تحديث الرصيد بعد بيع {symbol} - سيتم استخدام القيمة القديمة")
            else:
                fee_rate_sell = get_fee_rate(symbol, pos.symbol_type)
                fee_sell = (fill_price * filled_size) * fee_rate_sell
                sale_proceeds = fill_price * filled_size - fee_sell
                with _global_state_lock: bot_stats.last_balance += sale_proceeds
            holding_time_min = (datetime.now() - pos.open_time).total_seconds() / 60
            with _global_state_lock:
                if pnl_net > 0:
                    _daily_total_holding_time_win += holding_time_min
                    _daily_holding_count_win += 1
                else:
                    _daily_total_holding_time_loss += holding_time_min
                    _daily_holding_count_loss += 1
            with _global_state_lock:
                if DAILY_LOSS_MODE == 'net': _daily_loss_tracker = max(0, _daily_loss_tracker - pnl_net) if pnl_net > 0 else _daily_loss_tracker + abs(pnl_net)
                else:
                    if pnl_net < 0: _daily_loss_tracker += abs(pnl_net)
                pos.remaining_size -= filled_size
                pos.closed_pnl += pnl_net
                bot_stats.total_pnl_usdt += pnl_net
                bot_stats.daily_pnl += pnl_net
                log_trade(symbol, pos.side, pos.entry_price, fill_price, filled_size, pnl_net, reason, pos.pred, pos.confidence, pos.regime, pos.symbol_type, pos.ai_approved)
                position_value = pos.entry_price * filled_size
                pnl_percent = (pnl_net / position_value) * 100 if position_value != 0 else 0.0
                direction = "🔴" if pnl_net<0 else "🟢"
                sell_msg = (f"{direction} <b>بيع جزئي</b> {symbol}\nالنسبة: {percent*100:.0f}% | الحجم: {filled_size:.6f}\nالسعر: {fill_price:.8f} | الربح الصافي: {pnl_net:+.4f} USDT ({pnl_percent:+.2f}%)\nالسبب: {reason}")
                send_telegram(sell_msg)
                if pos.remaining_size <= 1e-8:
                    if pnl_net>0:
                        bot_stats.winning_trades += 1
                        _daily_winning_trades += 1
                    else:
                        bot_stats.losing_trades += 1
                        _daily_losing_trades += 1
                    bot_stats.total_trades += 1
                    _daily_trades_count += 1
                    
                    del open_positions[symbol]
                    with _cooldown_lock:
                        if pnl_net > 0:
                            cooldown_seconds = COOLDOWN_WIN_HOURS * 3600
                            _symbol_cooldown_until[symbol] = datetime.now() + timedelta(seconds=cooldown_seconds)
                            logger.info(f"⏳ تبريد {symbol}: ربح → {COOLDOWN_WIN_HOURS} ساعة")
                        else:
                            cooldown_seconds = COOLDOWN_LOSS_MINUTES * 60
                            _symbol_cooldown_until[symbol] = datetime.now() + timedelta(seconds=cooldown_seconds)
                            logger.info(f"⏳ تبريد {symbol}: خسارة → {COOLDOWN_LOSS_MINUTES} دقيقة")
                    _local_pending_symbols.discard(symbol)
                    _exchange_pending_symbols.discard(symbol)
                    total_eq = get_total_equity() if not TEST_MODE and not PAPER_TRADING and ENABLE_TRADING else bot_stats.last_balance
                    max_exp = total_eq * CUSTOM_MAX_EXPOSED_PERCENT
                    daily_limit = max_exp * MAX_DAILY_LOSS_PERCENT_OF_EXPOSED
                    if _daily_loss_tracker > daily_limit:
                        _set_paused(True)
                        daily_loss_cooldown_until = datetime.now() + timedelta(hours=COOLDOWN_HOURS_LOSS_LIMIT)
                        send_telegram(f"🚨 توقف بسبب الخسارة اليومية: {_daily_loss_tracker:.2f}")
                        generate_error_report("خطأ_حرج", "مراقبة", f"توقف للخسارة اليومية: {_daily_loss_tracker:.2f}")
                    bot_stats.add_equity_point(get_total_equity())
                save_state()
        finally:
            with _global_state_lock:
                if symbol in open_positions: open_positions[symbol]._closing = False
    except Exception as e:
        logger.error(f"خطأ غير متوقع في close_partial: {e}")
        generate_error_report("فشل_أمر", "أوامر", f"خطأ في close_partial: {e}")
        with _global_state_lock:
            if symbol in open_positions: open_positions[symbol]._closing = False

def close_all_positions(reason="أمر إداري"):
    with _global_state_lock:
        if not open_positions: send_telegram("ℹ️ لا توجد صفقات مفتوحة"); return
        syms = list(open_positions.keys())
    failed = []
    for sym in syms:
        ws_data = ws_manager.get_ticker(sym)
        if ws_data and ws_data.get('price', 0) > 0:
            price = ws_data['price']
        else:
            ticker = fetch_ticker_with_retry(sym, max_retries=1)
            price = ticker['last'] if ticker else None
        if price:
            close_partial(sym, 1.0, price, reason)
            time.sleep(0.5)
            with _global_state_lock:
                if sym in open_positions: failed.append(sym)
    if failed: send_telegram(f"<b>⚠️ فشل إغلاق:</b> {', '.join(failed)}"); generate_error_report("فشل_أمر", "أوامر", f"فشل إغلاق المراكز: {failed}")
    else: send_telegram(f"<b>🔒 تم إغلاق الكل.</b> السبب: {reason}")

def manual_resume():
    global daily_loss_cooldown_until, _daily_loss_tracker
    with _global_state_lock:
        _set_paused(False)
        daily_loss_cooldown_until = None
        warnings_list = []
        if _daily_loss_tracker > 0: warnings_list.append(f"⚠️ الخسارة اليومية (${_daily_loss_tracker:.2f}) لم تُمسح.")
        if warnings_list: send_telegram("<b>▶️ تم الاستئناف اليدوي</b>\n" + "\n".join(warnings_list))
        else: send_telegram("▶️ تم الاستئناف")
        save_state()

def retry_stuck_positions():
    while True:
        try:
            with _global_state_lock:
                stuck = {sym: pos for sym, pos in open_positions.items() if pos.last_fail_time is not None and pos.retry_count < MAX_STUCK_RETRIES}
            for sym, pos in stuck.items():
                retry_minutes = STUCK_POSITION_RETRY_MINUTES_MEME if pos.symbol_type == 'meme' else STUCK_POSITION_RETRY_MINUTES_NORMAL
                elapsed = (datetime.now() - pos.last_fail_time).total_seconds() / 60
                if elapsed < retry_minutes: continue
                logger.info(f"🔄 إعادة محاولة إغلاق {sym} ({pos.symbol_type}) بعد {elapsed:.1f}د (محاولة {pos.retry_count+1}/{MAX_STUCK_RETRIES})")
                ws_data = ws_manager.get_ticker(sym)
                if ws_data and ws_data.get('price', 0) > 0:
                    cur_price = ws_data['price']
                else:
                    ticker = fetch_ticker_with_retry(sym, max_retries=1)
                    cur_price = ticker['last'] if ticker else None
                if not cur_price: continue
                close_partial(sym, 1.0, cur_price, f"إعادة محاولة #{pos.retry_count+1} لإغلاق عالق")
                time.sleep(1)
            with _global_state_lock:
                for sym, pos in list(open_positions.items()):
                    if pos.retry_count >= MAX_STUCK_RETRIES and pos.last_fail_time is not None:
                        logger.error(f"❌ المركز {sym} فشل نهائياً بعد {MAX_STUCK_RETRIES} محاولات. يُنصح بالتدخل اليدوي.")
                        send_telegram(f"❌ المركز {sym} فشل إغلاقه بعد {MAX_STUCK_RETRIES} محاولات. تحقق يدوياً.")
                        generate_error_report("فشل_أمر", "أوامر", f"مركز عالق نهائياً: {sym}")
                        pos.last_fail_time = None
            time.sleep(30)
        except Exception as e:
            logger.error(f"خطأ في retry_stuck_positions: {e}")
            generate_error_report("فشل_خيط", "خيوط", f"retry_stuck_positions: {e}", traceback.format_exc())
            time.sleep(10)

def monitor_positions():
    global CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP, TRAILING_DISTANCE_PERCENT
    while True:
        try:
            with _global_state_lock: snapshot = list(open_positions.items())
            for sym, pos in snapshot:
                ws_data = ws_manager.get_ticker(sym)
                if ws_data and ws_data.get('price', 0) > 0:
                    cur_price = ws_data['price']
                else:
                    ticker = fetch_ticker_with_retry(sym, max_retries=1)
                    if ticker and ticker.get('last', 0) > 0:
                        cur_price = ticker['last']
                    else:
                        continue
                profit = pos.update(cur_price)
                
                # تم إزالة تحديث الوقف المسبق هنا تماماً
                
                if profit < 0:
                    if profit <= -STOP_LOSS_PARTIAL_1_PERCENT and not pos.sold_at_15:
                        close_partial(sym, 0.25, cur_price, 
                                      f"⚠️ بيع وقائي: خسارة {abs(profit):.2%} (بيع 25%) [عتبة {STOP_LOSS_PARTIAL_1_PERCENT:.2%}]")
                        pos.sold_at_15 = True
                        save_state()
                    elif profit <= -STOP_LOSS_PARTIAL_2_PERCENT and not pos.sold_at_20:
                        close_partial(sym, 0.33, cur_price, 
                                      f"⚠️ بيع وقائي: خسارة {abs(profit):.2%} (بيع 33%) [عتبة {STOP_LOSS_PARTIAL_2_PERCENT:.2%}]")
                        pos.sold_at_20 = True
                        save_state()
                    elif profit <= -STOP_LOSS_FULL_PERCENT:
                        if pos.remaining_size > 0:
                            close_partial(sym, 1.0, cur_price, 
                                          f"💀 بيع وقائي كامل: خسارة {abs(profit):.2%} (إغلاق كامل) [عتبة {STOP_LOSS_FULL_PERCENT:.2%}]")
                
                if pos.crash_monitor_start is None:
                    pos.crash_monitor_start = datetime.now()
                    pos.lowest_drop = 0.0
                else:
                    drop = (pos.entry_price - cur_price) / pos.entry_price if pos.side == 'buy' else (cur_price - pos.entry_price) / pos.entry_price
                    if drop > pos.lowest_drop:
                        pos.lowest_drop = drop
                    elapsed_seconds = (datetime.now() - pos.crash_monitor_start).total_seconds()
                    if elapsed_seconds <= 120 and drop >= 0.04:
                        close_partial(sym, 1.0, cur_price, "💥 انهيار مفاجئ >4% خلال دقيقتين (إغلاق كامل)")
                        continue
                
                if profit > 0.011:
                    now_time = datetime.now()
                    if pos.last_target_hit_index == -1:
                        if (now_time - pos.open_time).total_seconds() > (CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP * 60):
                            first_target = pos.take_profit_levels[0][0] if pos.take_profit_levels else None
                            if first_target and cur_price < first_target:
                                close_partial(sym, 1.0, cur_price, 
                                              f"🕒 انقضت {CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP} دقيقة دون تحقيق الهدف الأول – جني أرباح")
                                continue
                    else:
                        next_target_idx = pos.last_target_hit_index + 1
                        if next_target_idx < len(pos.take_profit_levels):
                            next_target = pos.take_profit_levels[next_target_idx][0]
                            if (now_time - pos.last_target_hit_time).total_seconds() > (CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP * 60) and cur_price < next_target:
                                close_partial(sym, 1.0, cur_price, 
                                              f"🕒 انقضت {CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP} دقيقة بعد تحقيق الهدف {pos.last_target_hit_index+1} دون الوصول للهدف التالي – جني أرباح")
                                continue
                
                tp_tolerance = 0.001
                tp_levels_copy = list(pos.take_profit_levels)
                executed = False
                for idx, (target, pct) in enumerate(tp_levels_copy):
                    if (pos.side == 'buy' and cur_price >= target * (1 - tp_tolerance)) or \
                       (pos.side == 'sell' and cur_price <= target * (1 + tp_tolerance)):
                        pos.last_target_hit_time = datetime.now()
                        pos.last_target_hit_index = idx
                        pos.tp_hit_count += 1
                        target_number = pos.tp_hit_count
                        if idx >= 5:
                            close_partial(sym, 1.0, cur_price, f"🎯 جني أرباح كامل (الهدف {target_number})")
                        else:
                            close_partial(sym, pct, cur_price, f"🎯 جني أرباح جزئي (الهدف {target_number})")
                        if [target, pct] in pos.take_profit_levels:
                            pos.take_profit_levels.remove([target, pct])
                        executed = True
                        break
                if executed:
                    continue
                
                if pos.stop_loss:
                    sl = pos.stop_loss * (0.997 if pos.side=='buy' else 1.003)
                    if (pos.side=='buy' and cur_price <= sl) or (pos.side=='sell' and cur_price >= sl):
                        close_partial(sym, 1.0, cur_price, "🛑 وقف خسارة")
                        continue
                
                if profit <= -0.06:
                    close_partial(sym, 1.0, cur_price, "💥 أقصى خسارة 6%")
                    continue
            
            time.sleep(POSITION_MONITOR_INTERVAL)
        except Exception as e:
            logger.error(f"خطأ في monitor_positions: {e}")
            generate_error_report("فشل_خيط", "مراقبة", f"monitor_positions: {e}", traceback.format_exc())
            time.sleep(5)

def check_liquidity(symbol, size_usd, sym_type='normal'):
    ob = fetch_orderbook_with_cache(symbol, limit=LIQUIDITY_CHECK_DEPTH)
    if not ob: return False, "لا يمكن جلب دفتر الأوامر"
    best_bid = ob['bids'][0][0] if ob['bids'] else 0
    best_ask = ob['asks'][0][0] if ob['asks'] else 0
    if best_bid == 0 or best_ask == 0: return False, "لا يوجد سعر في الدفتر"
    spread = (best_ask - best_bid) / best_bid
    max_spread = MAX_SPREAD_MEME if sym_type == 'meme' else MAX_SPREAD_NORMAL
    if spread > max_spread: return False, f"فارق سعر كبير جداً: {spread:.4f} > {max_spread}"
    mid_price = (best_bid + best_ask) / 2
    depth_5pct_up = sum(a[0] * a[1] for a in ob['asks'] if a[0] <= mid_price * 1.05)
    depth_5pct_down = sum(b[0] * b[1] for b in ob['bids'] if b[0] >= mid_price * 0.95)
    min_depth = MIN_DEPTH_USD_MEME if sym_type == 'meme' else MIN_DEPTH_USD_NORMAL
    required_depth = size_usd * 1.2
    if depth_5pct_up < required_depth or depth_5pct_down < required_depth:
        return False, f"عمق غير كافٍ (up={depth_5pct_up:.0f}, down={depth_5pct_down:.0f}, need={required_depth:.0f})"
    total_depth_up = sum(a[0] * a[1] for a in ob['asks'][:LIQUIDITY_CHECK_DEPTH])
    total_depth_down = sum(b[0] * b[1] for b in ob['bids'][:LIQUIDITY_CHECK_DEPTH])
    max_percent = LIQUIDITY_MAX_PERCENT
    if size_usd / total_depth_up > max_percent or size_usd / total_depth_down > max_percent:
        return False, f"حجم الصفقة كبير جداً نسبة للعمق (حجم/عمق > {max_percent:.0%})"
    return True, "السيولة كافية"

def execute_full_trade(symbol, side, size, price, atr, sl, tp, sym_type, pred, conf, regime, ai_approved=False, approval_reason="",
                        scores_5m=None, scores_15m=None, scores_1h=None,
                        weighted_score_5m=None, weighted_score_15m=None, weighted_score_1h=None,
                        final_score=None):
    if not _global_state_lock.acquire(timeout=3):
        logger.warning(f"⚠️ فشل الحصول على _global_state_lock في execute_full_trade لـ {symbol}")
        return False, 0
    try:
        if symbol in open_positions: return False, 0
        if symbol in _local_pending_symbols or symbol in _exchange_pending_symbols: return False, 0
        _local_pending_symbols.add(symbol)
    finally: _global_state_lock.release()
    try:
        min_amt, max_amt, min_cost = get_amount_limits(symbol)
        if size < min_amt: return False, 0
        if max_amt and size > max_amt: size = max_amt
        if min_cost and size * price < min_cost: return False, 0
        if _should_simulate():
            if TEST_MODE or PAPER_TRADING:
                fee_rate_buy = get_fee_rate(symbol, sym_type)
                fee_buy = (size * price) * fee_rate_buy
                total_cost = size * price + fee_buy
                with _global_state_lock:
                    if bot_stats.last_balance < total_cost:
                        logger.warning(f"⚠️ رصيد غير كافٍ في المحاكاة الورقية: {bot_stats.last_balance:.2f} < {total_cost:.2f}")
                        return False, 0
                    bot_stats.last_balance -= total_cost
            pos = Position(symbol, side, size, price, atr, sl, tp, sym_type, pred, conf, regime, ai_approved=ai_approved,
                           scores_5m=scores_5m, scores_15m=scores_15m, scores_1h=scores_1h,
                           weighted_score_5m=weighted_score_5m, weighted_score_15m=weighted_score_15m,
                           weighted_score_1h=weighted_score_1h, final_score=final_score)
            with _global_state_lock: open_positions[symbol] = pos
            send_telegram(f"🧪 [{'محاكاة' if TEST_MODE else 'ورقي'}] شراء {symbol} ({sym_type}) | الحجم: {size:.6f} | السعر: {price:.8f}\n📊 سبب الموافقة: {approval_reason}")
            save_state()
            return True, size
        success, fill_price, filled_size, _ = execute_limit_order(symbol, side, size, price, sym_type)
        if success and fill_price and filled_size>0:
            # تم إزالة إنشاء الوقف المسبق بالكامل
            pos = Position(symbol, side, filled_size, fill_price, atr, sl, tp, sym_type, pred, conf, regime, ai_approved=ai_approved,
                           scores_5m=scores_5m, scores_15m=scores_15m, scores_1h=scores_1h,
                           weighted_score_5m=weighted_score_5m, weighted_score_15m=weighted_score_15m,
                           weighted_score_1h=weighted_score_1h, final_score=final_score)
            with _global_state_lock: open_positions[symbol] = pos
            new_balance = _safe_fetch_balance_after_trade(attempts=10, delay=3.0, silent=True)
            if new_balance is not None:
                with _global_state_lock: bot_stats.last_balance = new_balance
            send_telegram(f"✅ <b>شراء</b> {symbol} ({sym_type}) | الحجم: {filled_size:.6f} | السعر: {fill_price:.8f}\n📊 سبب الموافقة: {approval_reason}")
            save_state()
            return True, filled_size
        else: return False, 0
    except Exception as e:
        logger.error(f"خطأ غير متوقع في execute_full_trade لـ {symbol}: {e}")
        generate_error_report("فشل_أمر", "تنفيذ", str(e))
        return False, 0
    finally:
        with _global_state_lock: _local_pending_symbols.discard(symbol)

# --------------------------- خيط auto_recovery ---------------------------
def auto_recovery_monitor():
    global _auto_recovery_failures, rest_rate_limiter
    exchange = get_active_exchange()
    test_symbol = 'BTC/USDT'
    consecutive_failures = 0
    first_failure_time = None
    recovery_attempts_remaining = 0
    while True:
        ticker = None
        try:
            ticker = exchange.fetch_ticker(test_symbol)
            if ticker and ticker.get('last') is not None:
                if consecutive_failures > 0:
                    logger.info("✅ استعاد البوت الاتصال بـ Binance تلقائياً.")
                consecutive_failures = 0
                first_failure_time = None
                recovery_attempts_remaining = 0
                with _AUTO_RECOVERY_LOCK:
                    _auto_recovery_failures = 0
                time.sleep(random.uniform(60, 300))
                continue
        except Exception as e:
            logger.warning(f"⚠️ فشل اختبار الاتصال: {e}")
        consecutive_failures += 1
        if first_failure_time is None:
            first_failure_time = time.time()
            logger.info("⏳ تم اكتشاف أول فشل. سأنتظر 12 دقيقة قبل محاولة الاسترداد.")
            recovery_attempts_remaining = 0
            time.sleep(random.uniform(60, 300))
            continue
        elapsed = time.time() - first_failure_time
        if elapsed < 720:
            logger.debug(f"⏳ انتظار مرور 12 دقيقة (مضى {elapsed:.0f} ثانية)")
            time.sleep(random.uniform(60, 300))
            continue
        if recovery_attempts_remaining == 0:
            logger.info("🔁 مضى 12 دقيقة من الفشل المستمر. سأقوم بـ 3 محاولات جديدة للتحقق.")
            recovery_attempts_remaining = 3
        if recovery_attempts_remaining > 0:
            time.sleep(random.uniform(10, 30))
            recovery_attempts_remaining -= 1
            if recovery_attempts_remaining == 0:
                logger.critical("❌ فشلت المحاولات الثلاث بعد 12 دقيقة. بدء إجراء الاسترداد التلقائي...")
                send_telegram("🔄 فشل الاتصال بـ Binance لمدة 12 دقيقة و 3 محاولات - جاري الاسترداد التلقائي (إعادة ضبط RateLimiter و WebSocket).")
                try:
                    global rest_rate_limiter
                    rest_rate_limiter = RateLimiter(max_weight=1200, period=60)
                    logger.info("تم إعادة تعيين RateLimiter (max_weight=1200).")
                    ws_manager.restart()
                    logger.info("تم إعادة تشغيل WebSocket Manager.")
                    with _cache_lock:
                        _ohlcv_cache.clear()
                    with _features_cache_lock:
                        _features_cache.clear()
                    with _orderbook_cache_lock:
                        _orderbook_cache.clear()
                    logger.info("تم مسح جميع ذاكرات التخزين المؤقت.")
                    send_telegram("✅ الاسترداد التلقائي ناجح (تم إعادة ضبط RateLimiter و WebSocket والكاش). البوت يعمل مجدداً.")
                    consecutive_failures = 0
                    first_failure_time = None
                    recovery_attempts_remaining = 0
                    with _AUTO_RECOVERY_LOCK:
                        _auto_recovery_failures = 0
                except Exception as rec_err:
                    logger.error(f"خطأ أثناء الاسترداد التلقائي: {rec_err}")
                    send_telegram(f"⚠️ فشل الاسترداد التلقائي: {rec_err}")
                    recovery_attempts_remaining = 0
                    time.sleep(random.uniform(120, 300))
            continue
        else:
            time.sleep(random.uniform(60, 300))

# --------------------------- دالة التحليل الرئيسية ---------------------------
def analyze_and_trade():
    global bot_stats, _daily_loss_tracker, _daily_trades_count
    global _daily_trades_date, _daily_winning_trades, _daily_losing_trades
    global daily_loss_cooldown_until
    global _last_processing_lock_released
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    global _daily_biggest_win, _daily_biggest_loss, _daily_most_traded
    global buying_committee, _analysis_failures
    global PAUSE_NEW_ENTRIES, USE_1H_MULTIPLIER, PAUSE_ANALYSIS, UPPER_THRESHOLD_GLOBAL
    global _last_analyzed_candidates, _prev_active_symbols_limit
    global PAUSED, last_analysis_time

    if PAUSE_ANALYSIS:
        logger.info("⏸️ التحليل موقف مؤقتاً (PAUSE_ANALYSIS=True)")
        return

    if CUSTOM_ACTIVE_SYMBOLS_LIMIT <= 0:
        logger.info("⏸️ عدد رموز التحليل = 0، تخطي التحليل.")
        return

    if CUSTOM_ACTIVE_SYMBOLS_LIMIT > 0 and _prev_active_symbols_limit == 0:
        logger.info("🔄 تم تغيير عدد الرموز من 0 إلى قيمة موجبة، إعادة ضبط المسح والتحليل.")
        if scanner:
            scanner.last_scan = 0
            scanner.next_scan_time = 0
        force_unlock()
        with _last_analysis_time_lock:
            last_analysis_time = datetime.now() - timedelta(minutes=5)
        PAUSED = False
        PAUSE_ANALYSIS = False
        PAUSE_NEW_ENTRIES = False
        _prev_active_symbols_limit = CUSTOM_ACTIVE_SYMBOLS_LIMIT
        send_telegram("🔄 تم إعادة ضبط المسح والتحليل بعد الخروج من حالة الصفر.")

    if scanner and (not scanner.candidates or time.time() - scanner.last_scan > 3600):
        logger.info("⚠️ قائمة المرشحين فارغة أو قديمة (أكثر من ساعة)، نقوم بمسح فوري (REST احتياطي).")
        scanner.scan()

    lock_acquired = False
    try:
        lock_acquired = _processing_lock.acquire(timeout=30)
        if not lock_acquired:
            _analysis_failures += 1
            logger.warning(f"⚠️ فشل الحصول على القفل (المحاولة {_analysis_failures}/{_MAX_CONSECUTIVE_FAILURES})")
            if _analysis_failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.critical(f"❌ {_MAX_CONSECUTIVE_FAILURES} محاولات فاشلة متتالية - تحرير القفل القسري")
                force_unlock()
                _analysis_failures = 0
            return
        _analysis_failures = 0
        
        elapsed_since_start = time.time() - _bot_start_time
        if elapsed_since_start < STARTUP_DELAY_SECONDS:
            logger.info(f"⏳ بدء التشغيل: انتظار {STARTUP_DELAY_SECONDS - elapsed_since_start:.0f} ثانية قبل بدء التحليل")
            return

        effective_limit = CUSTOM_ACTIVE_SYMBOLS_LIMIT
        if elapsed_since_start < (STARTUP_DELAY_SECONDS + STARTUP_REDUCED_LIMIT_DURATION):
            effective_limit = STARTUP_REDUCED_SYMBOLS
        with _last_analysis_time_lock: _last_analysis_time = datetime.now()
        if _is_paused():
            now = datetime.now()
            if daily_loss_cooldown_until and now >= daily_loss_cooldown_until:
                _set_paused(False)
                daily_loss_cooldown_until = None
                _daily_loss_tracker = 0.0
                _daily_trades_count = 0
                _daily_winning_trades = 0
                _daily_losing_trades = 0
                send_telegram("✅ تم استئناف التداول (انتهاء الخسارة اليومية)")
                save_state()
            else: return
        today = datetime.now().date()
        with _global_state_lock:
            if _daily_trades_date != today:
                if _daily_trades_count > 0:
                    wr = (_daily_winning_trades / _daily_trades_count * 100) if _daily_trades_count > 0 else 0
                    avg_holding_win = (_daily_total_holding_time_win / _daily_holding_count_win) if _daily_holding_count_win > 0 else 0
                    avg_holding_loss = (_daily_total_holding_time_loss / _daily_holding_count_loss) if _daily_holding_count_loss > 0 else 0
                    most_traded = max(_daily_most_traded.items(), key=lambda x: x[1]) if _daily_most_traded else ("لا يوجد", 0)
                    report = (f"<b>📊 تقرير يومي مفصل</b> ({today})\n📈 عدد الصفقات: {_daily_trades_count} | ✅ رابحة: {_daily_winning_trades} | ❌ خاسرة: {_daily_losing_trades}\n🏆 نسبة النجاح: {wr:.1f}%\n💰 <b>صافي الربح/الخسارة اليومي:</b> ${bot_stats.daily_pnl:.2f}\n🌟 أكبر ربح: ${_daily_biggest_win:.2f} | 💀 أكبر خسارة: ${_daily_biggest_loss:.2f}\n🕒 متوسط زمن الاحتفاظ (رابح): {avg_holding_win:.1f} دقيقة | (خاسر): {avg_holding_loss:.1f} دقيقة\n🔄 أكثر عملة تداولاً: {most_traded[0]} ({most_traded[1]} صفقة)")
                    send_telegram(report)
                _daily_trades_date = today
                _daily_loss_tracker = 0.0
                _daily_trades_count = 0
                _daily_winning_trades = 0
                _daily_losing_trades = 0
                _daily_biggest_win = 0.0
                _daily_biggest_loss = 0.0
                _daily_most_traded.clear()
                _daily_total_holding_time_win = 0.0
                _daily_total_holding_time_loss = 0.0
                _daily_holding_count_win = 0
                _daily_holding_count_loss = 0
                bot_stats.daily_pnl = 0.0
                save_state()

        current_week = datetime.now().isocalendar()[1]
        if bot_stats.last_week_number != current_week:
            send_telegram(f"<b>📊 تقرير أسبوعي</b>\nالأسبوع {bot_stats.last_week_number} → {current_week}\n📈 ربح الأسبوع الماضي: ${bot_stats.weekly_pnl:.2f}\n🏆 انتصارات: {bot_stats.weekly_wins} | ❌ هزائم: {bot_stats.weekly_losses}")
            bot_stats.weekly_pnl = 0.0
            bot_stats.weekly_wins = 0
            bot_stats.weekly_losses = 0
            bot_stats.last_week_number = current_week
            save_state()

        total_eq = get_total_equity()
        with _global_state_lock:
            free_bal = bot_stats.last_balance
        if free_bal is None or free_bal < 10:
            logger.warning("⚠️ الرصيد المخزن أقل من 10$، انتظار تحديث الخلفية...")
            return

        position_percent = POSITION_SIZE_PERCENT
        max_pos_usdt = total_eq * position_percent

        if scanner.last_scan == 0 and elapsed_since_start < INITIAL_SCAN_DELAY:
            logger.info(f"⏳ تأجيل أول مسح للسوق لمدة {INITIAL_SCAN_DELAY - elapsed_since_start:.0f} ثانية")
        elif scanner.should_scan(): scanner.scan()

        candidates = scanner.candidates
        seen = set()
        unique_candidates = []
        for sym in candidates:
            if sym not in seen:
                seen.add(sym)
                unique_candidates.append(sym)

        forbidden = set(open_positions.keys()).union(_local_pending_symbols).union(_exchange_pending_symbols)
        available = [sym for sym in unique_candidates if sym not in forbidden]

        if not available:
            selected_candidates = []
        else:
            desired_count = 12
            if len(available) <= desired_count:
                selected_candidates = available
            else:
                first_5 = available[:5]
                last_5 = _last_analyzed_candidates[:5] if _last_analyzed_candidates else []
                if set(first_5) == set(last_5) and len(last_5) == 5:
                    next_12 = available[12:24]
                    if len(next_12) >= desired_count:
                        selected_candidates = next_12[:desired_count]
                    elif next_12:
                        selected_candidates = next_12 + available[:desired_count - len(next_12)]
                    else:
                        selected_candidates = available[:desired_count]
                else:
                    selected_candidates = available[:desired_count]
                    _last_analyzed_candidates = selected_candidates

        open_syms = list(open_positions.keys())
        active = open_syms.copy()
        remaining = effective_limit - len(active)
        if remaining > 0:
            for sym in selected_candidates:
                if sym not in active:
                    active.append(sym)
                    remaining -= 1
                    if remaining == 0:
                        break

        all_tickers = fetch_tickers_with_retry()
        if not all_tickers:
            with _global_state_lock: all_tickers = dict(bot_stats.last_valid_tickers)
            if not all_tickers:
                logger.warning("⚠️ لا توجد بيانات أسعار متاحة، تخطي التحليل")
                return
        curr_exp = 0.0
        with _global_state_lock:
            for sym, p in open_positions.items():
                tick = all_tickers.get(sym)
                price = tick['last'] if tick else p.entry_price
                curr_exp += p.remaining_size * price
        curr_exp += get_pending_exposure_estimate()
        max_allowed_exposure = total_eq * CUSTOM_MAX_EXPOSED_PERCENT
        buy_comm = buying_committee
        with _global_state_lock: forbidden = set(open_positions.keys()).union(_local_pending_symbols).union(_exchange_pending_symbols)

        pending = []
        for sym in active:
            if sym in forbidden: continue
            with _cooldown_lock:
                if sym in _symbol_cooldown_until:
                    cooldown_end = _symbol_cooldown_until[sym]
                    if datetime.now() < cooldown_end:
                        remaining = (cooldown_end - datetime.now()).total_seconds()
                        logger.debug(f"⏳ {sym} في فترة تبريد: {remaining/60:.1f} دقيقة متبقية")
                        continue
                    else:
                        del _symbol_cooldown_until[sym]
            
            regime = detect_market_regime(sym)
            if regime == 'trending_down': continue
            if sym in open_positions:
                ttl_used = 30
            else:
                ttl_used = 120
            
            df_5m = get_cached_features(sym, '5m', limit=200, ttl=ttl_used)
            df_15m = get_cached_features(sym, '15m', limit=500, ttl=ttl_used)
            df_1h = get_cached_features(sym, '1h', limit=500, ttl=ttl_used)

            if df_5m.empty:
                df_5m = df_15m
            if df_1h.empty:
                df_1h = df_15m
            if df_15m.empty or len(df_15m) < 50:
                continue
            time.sleep(random.uniform(0.2, 0.6))
            
            sym_type = classify_symbol(sym, df=df_15m, ticker_data=all_tickers.get(sym))
            
            dec, avg_score, conf, reason, avg_scores, scores_5m, scores_15m, scores_1h = buy_comm.decide(df_5m, df_15m, df_1h, sym, sym_type, 'normal')
            if dec != 'buy':
                continue

            if not df_5m.empty and len(df_5m) >= 20 and not df_15m.empty and len(df_15m) >= 20:
                res_5m = buy_comm.calculate_weighted_average(df_5m, None, None, sym, sym_type, 'normal')
                score_5m = res_5m['weighted_avg']
                res_15m = buy_comm.calculate_weighted_average(df_15m, None, None, sym, sym_type, 'normal')
                score_15m = res_15m['weighted_avg']
                multiplier = get_multiplier_from_score(score_5m, score_15m)
            else:
                res_5m = buy_comm.calculate_weighted_average(df_5m, None, None, sym, sym_type, 'normal')
                score_5m = res_5m['weighted_avg']
                multiplier = get_multiplier_from_score(score_5m)

            final_score = avg_score * multiplier

            if (sym_type == 'meme' and final_score < SCALP_MIN_PROFIT) or (sym_type != 'meme' and final_score < STRENGTH_THRESHOLD):
                continue
            if final_score > UPPER_THRESHOLD_GLOBAL:
                continue
            
            ticker = all_tickers.get(sym)
            if not ticker: continue
            cur_price = ticker.get('last')
            if cur_price is None or cur_price <= 0: continue
            
            if FILTER_HOUR_CANDLE_ENABLED:
                df_1h_filter = fetch_ohlcv_persistent(sym, '1h', limit=2, max_attempts=5, retry_interval=10)
                if df_1h_filter.empty or len(df_1h_filter) < 2: continue
                last_completed_close = df_1h_filter['close'].iloc[-2]
                if cur_price <= last_completed_close: continue
            
            atr_val = df_15m['atr'].iloc[-1] if 'atr' in df_15m and not df_15m['atr'].isna().all() else cur_price * 0.02
            max_sl = MAX_SL_PERCENT_MEME if sym_type == 'meme' else MAX_SL_PERCENT_NORMAL
            sl_price, tp_price = dynamic_stop_loss_take_profit(cur_price, atr_val, 'buy', regime, sym_type, max_sl)
            size = max_pos_usdt / cur_price
            min_amt, max_amt, min_cost = get_amount_limits(sym)
            if size < min_amt: continue
            if max_amt and size > max_amt: size = max_amt
            if min_cost and size * cur_price < min_cost: continue
            if curr_exp + size * cur_price > max_allowed_exposure: continue
            if free_bal < size * cur_price: continue
            if FILTER_LIQUIDITY_ENABLED:
                liquidity_ok, liq_msg = check_liquidity(sym, size * cur_price, sym_type)
                if not liquidity_ok:
                    logger.info(f"تم رفض {sym} بسبب عدم كفاية السيولة: {liq_msg}")
                    now = time.time()
                    last_key = f"liq_reject_{sym}"
                    if last_key not in _last_error_report_times or now - _last_error_report_times.get(last_key, 0) > 600:
                        send_telegram(f"⚠️ تم رفض {sym} بسبب السيولة: {liq_msg}")
                        _last_error_report_times[last_key] = now
                    continue
            
            cfg = buy_comm.thresholds.get(sym_type, {}).get('normal', buy_comm.thresholds['normal']['normal'])
            thresh = cfg.get('thresh1', 0.45)
            
            r5, e5, c5, t5, v5 = scores_5m['rule'], scores_5m['emr'], scores_5m['cfhm'], scores_5m['timing'], scores_5m['vwap_obv']
            r15, e15, c15, t15, v15 = scores_15m['rule'], scores_15m['emr'], scores_15m['cfhm'], scores_15m['timing'], scores_15m['vwap_obv']
            r1h, e1h, c1h, t1h, v1h = scores_1h['rule'], scores_1h['emr'], scores_1h['cfhm'], scores_1h['timing'], scores_1h['vwap_obv']
            avg_r, avg_e, avg_c, avg_t, avg_v = avg_scores['rule'], avg_scores['emr'], avg_scores['cfhm'], avg_scores['timing'], avg_scores['vwap_obv']
            weights_str = f"أوزان الأطر: 5م={WEIGHT_5M:.2f}% | 15د={WEIGHT_15M:.2f}% | 1س={WEIGHT_1H:.2f}%"
            
            weighted_5m = sum(scores_5m[m] * buy_comm.weights[m] for m in buy_comm.weights)
            weighted_15m = sum(scores_15m[m] * buy_comm.weights[m] for m in buy_comm.weights)
            weighted_1h = sum(scores_1h[m] * buy_comm.weights[m] for m in buy_comm.weights)

            reason = (
                f"✅ موافقة: المتوسط النهائي={avg_score:.3f} | العتبة={thresh:.3f} | المضاعف={multiplier:.2f}x\n"
                f"⚖️ {weights_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 إطار 5 دقائق:\n"
                f"   Rule={r5:.3f} | EMR={e5:.3f} | CFHM={c5:.3f} | Timing={t5:.3f} | VWAP={v5:.3f}\n"
                f"📊 إطار 15 دقيقة:\n"
                f"   Rule={r15:.3f} | EMR={e15:.3f} | CFHM={c15:.3f} | Timing={t15:.3f} | VWAP={v15:.3f}\n"
                f"📊 إطار 1 ساعة:\n"
                f"   Rule={r1h:.3f} | EMR={e1h:.3f} | CFHM={c1h:.3f} | Timing={t1h:.3f} | VWAP={v1h:.3f}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📈 المتوسط الحسابي (للأعضاء عبر الأطر):\n"
                f"   Rule={avg_r:.3f} | EMR={avg_e:.3f} | CFHM={avg_c:.3f} | Timing={avg_t:.3f} | VWAP={avg_v:.3f}"
            )
            
            pending.append({
                'symbol': sym,
                'score': final_score,
                'price': cur_price,
                'atr': atr_val,
                'sl': sl_price,
                'tp': tp_price,
                'size': size,
                'type': sym_type,
                'pred': avg_score,
                'conf': avg_score,
                'regime': regime,
                'df_primary': df_15m,
                'reason': reason,
                'scores_5m': scores_5m,
                'scores_15m': scores_15m,
                'scores_1h': scores_1h,
                'weighted_5m': weighted_5m,
                'weighted_15m': weighted_15m,
                'weighted_1h': weighted_1h,
                'final_score': final_score
            })
        
        if pending:
            pending.sort(key=lambda x: x['score'], reverse=True)
            rem_bal = free_bal
            exp = curr_exp
            total_eq2 = get_total_equity()
            max_exp2 = total_eq2 * CUSTOM_MAX_EXPOSED_PERCENT
            pos_usdt = total_eq2 * POSITION_SIZE_PERCENT
            
            if PAUSE_NEW_ENTRIES:
                logger.info("⏸️ دخول الصفقات الجديدة موقف مؤقتاً (PAUSE_NEW_ENTRIES = True).")
                return

            for opp in pending:
                with _global_state_lock:
                    if _daily_trades_count >= CUSTOM_MAX_DAILY_TRADES: break
                    if opp['symbol'] in open_positions or opp['symbol'] in _local_pending_symbols or opp['symbol'] in _exchange_pending_symbols: continue
                approved = True
                sz = pos_usdt / opp['price']
                min_amt2, max_amt2, min_cost2 = get_amount_limits(opp['symbol'])
                if sz < min_amt2: continue
                if max_amt2 and sz > max_amt2: sz = max_amt2
                if min_cost2 and sz * opp['price'] < min_cost2: continue
                if exp + pos_usdt > max_exp2: continue
                if rem_bal < pos_usdt: continue
                succ, actual_size = execute_full_trade(opp['symbol'], 'buy', sz, opp['price'], opp['atr'],
                                                       opp['sl'], opp['tp'], opp['type'], opp['pred'],
                                                       opp['conf'], opp['regime'], ai_approved=approved,
                                                       approval_reason=opp.get('reason', ''),
                                                       scores_5m=opp.get('scores_5m'),
                                                       scores_15m=opp.get('scores_15m'),
                                                       scores_1h=opp.get('scores_1h'),
                                                       weighted_score_5m=opp.get('weighted_5m'),
                                                       weighted_score_15m=opp.get('weighted_15m'),
                                                       weighted_score_1h=opp.get('weighted_1h'),
                                                       final_score=opp.get('final_score'))
                if succ:
                    new_rem = _safe_fetch_balance_after_trade(attempts=10, delay=3.0, silent=True) if not PAPER_TRADING else bot_stats.last_balance
                    if new_rem is None:
                        logger.error("⚠️ فشل تحديث الرصيد بعد الصفقة. إيقاف معالجة الفرص المتبقية.")
                        send_telegram("⚠️ فشل تحديث الرصيد بعد الصفقة. تم إيقاف تنفيذ المزيد من الفرص مؤقتاً.")
                        break
                    rem_bal = new_rem
                    exp += actual_size * opp['price']
    except TimeoutError:
        logger.warning("⚠️ مهلة الحصول على قفل التحليل، تخطي الدورة")
    except Exception as e:
        generate_error_report("خطأ_حرج", "تحليل", str(e), traceback.format_exc())
    finally:
        if lock_acquired:
            try:
                _processing_lock.release()
            except Exception as e:
                logger.error(f"خطأ أثناء تحرير القفل: {e}")
            _last_processing_lock_released = time.time()

# --------------------------- دوال الخلفية والمراقبة ---------------------------
def health_monitor():
    stuck_start = None; last_alert_time = 0
    while True:
        time.sleep(30)
        if _processing_lock.locked():
            if stuck_start is None: stuck_start = datetime.now()
            else:
                elapsed = (datetime.now() - stuck_start).total_seconds()
                if elapsed > 480 and time.time() - last_alert_time > 1800:
                    logger.critical("⚠️ قفل التحليل عالق لأكثر من 8 دقائق! قد يحتاج تدخل يدوي.")
                    send_telegram("⚠️ تحذير: قفل التحليل محتجز منذ 8 دقائق، جاري محاولة التحرير القسري.")
                    force_unlock()
                    last_alert_time = time.time()
        else: stuck_start = None

def lock_health_monitor():
    last_release = time.time()
    while True:
        time.sleep(30)
        now = time.time()
        if _processing_lock.locked():
            elapsed = now - last_release
            if elapsed > LOCK_TIMEOUT_SECONDS:
                logger.critical(f"❗ قفل _processing_lock معلق لمدة {elapsed:.0f} ثانية! (أكثر من {LOCK_TIMEOUT_SECONDS} ثانية) جاري التحرير القسري.")
                send_telegram(f"⚠️ قفل التحليل عالق لمدة {elapsed:.0f} ثانية - تحرير قسري")
                force_unlock()
                last_release = time.time()
        else: last_release = now

def sync_pending_orders():
    global _exchange_pending_symbols
    if TEST_MODE or PAPER_TRADING or not ENABLE_TRADING: return
    exchange = get_active_exchange()
    try:
        rest_rate_limiter.wait_if_needed(weight=2)
        orders = exchange.fetch_open_orders()
        pending_from_exchange = set()
        now_ts = datetime.now()
        for order in orders:
            sym = order['symbol']
            pending_from_exchange.add(sym)
            ts = order.get('timestamp')
            if isinstance(ts, (int, float)): order_time = datetime.fromtimestamp(ts/1000)
            elif isinstance(ts, datetime): order_time = ts
            else: continue
            if (now_ts - order_time).total_seconds() > 600 and sym not in open_positions:
                try:
                    rest_rate_limiter.wait_if_needed(weight=2)
                    exchange.cancel_order(order['id'], sym)
                    logger.info(f"تم إلغاء أمر قديم لـ {sym}")
                except Exception as e: logger.warning(f"فشل إلغاء الأمر القديم {sym}: {e}")
        with _global_state_lock:
            _exchange_pending_symbols.clear()
            _exchange_pending_symbols.update(pending_from_exchange)
    except Exception as e: logger.warning(f"فشل مزامنة الأوامر: {e}")

def periodic_sync_pending():
    time.sleep(30)
    while True:
        sync_pending_orders()
        time.sleep(3 * 3600)  # 3 ساعات

def balance_recovery_monitor():
    global _balance_failure_paused, _balance_retry_count
    long_failure_cycles = 0
    while True:
        time.sleep(60)
        if not _balance_failure_paused: long_failure_cycles = 0; continue
        _balance_retry_count += 1
        if _balance_retry_count <= MAX_BALANCE_RETRIES:
            bal = get_real_balance_usdt(max_retries=5, delay=5, silent=True)
            if bal is not None and bal > 0:
                _balance_retry_count = 0; long_failure_cycles = 0; logger.info("✅ تم استعادة الرصيد بعد محاولات متعددة.")
            else: time.sleep(120)
            continue
        long_failure_cycles += 1; _balance_retry_count = 0
        if long_failure_cycles <= 3:
            logger.warning(f"⚠️ الدورة الطويلة #{long_failure_cycles}: فشل الرصيد لأكثر من 20 محاولة.")
            if long_failure_cycles == 1: send_telegram("⏳ فشل استعادة الرصيد لمدة طويلة. سأحاول مجدداً بعد ساعة.")
            time.sleep(3600)
        else:
            logger.critical("❌ فشل دائم في استعادة الرصيد.")
            send_telegram("❌ فشل دائم في استعادة الرصيد. تحقق من API أو المنصة.")
            generate_error_report("فشل_اتصال", "رصيد", "فشل دائم في استعادة الرصيد")
            time.sleep(43200)
            long_failure_cycles = 0; _balance_retry_count = 0

def self_heartbeat():
    time.sleep(30)
    port = int(os.environ.get('PORT', 8080))
    while True:
        try: requests.get(f"http://localhost:{port}/ping", timeout=5)
        except: pass
        time.sleep(300)

def background_scanner():
    time.sleep(INITIAL_SCAN_DELAY)
    while True:
        if not PAUSE_ANALYSIS and scanner and scanner.should_scan():
            scanner.scan()
        time.sleep(10)

def background_analyzer():
    time.sleep(STARTUP_DELAY_SECONDS)
    while True:
        if PAUSE_ANALYSIS:
            time.sleep(10)
            continue
        analyze_and_trade()
        time.sleep(100)

def background_balance_updater():
    time.sleep(120)
    while True:
        if ENABLE_TRADING and not TEST_MODE and not PAPER_TRADING:
            bal = get_real_balance_usdt(max_retries=3, delay=2, silent=True)
            if bal is not None:
                with _global_state_lock:
                    bot_stats.last_balance = bal
                    _last_successful_balance_time = time.time()
                logger.info(f"✅ تحديث الرصيد الخلفي: ${bal:.2f}")
            else:
                logger.warning("⚠️ فشل تحديث الرصيد الخلفي، الاحتفاظ بالقيمة القديمة")
        sleep_duration = random.uniform(540, 600)
        time.sleep(sleep_duration)

def cache_cleanup_thread():
    last_full_cleanup = 0
    while True:
        time.sleep(300)
        now = time.time()
        with _cache_lock:
            items = list(_ohlcv_cache.items())
            if len(items) > _ohlcv_cache_max:
                for key, _ in items[:20]:
                    if key in _ohlcv_cache: del _ohlcv_cache[key]
        with _features_cache_lock:
            items = list(_features_cache.items())
            if len(items) > _features_cache_max:
                for key, _ in items[:30]:
                    if key in _features_cache: del _features_cache[key]
        if now - last_full_cleanup > 1800:
            last_full_cleanup = now
            cleanup_error_reports()
            with _market_cap_cache_lock:
                keys_to_delete = [coin for coin, entry in _market_cap_cache.items() if now - entry['timestamp'] > 86400]
                for coin in keys_to_delete[:20]: del _market_cap_cache[coin]
            logger.debug("🧹 تنظيف عميق للذاكرة")

def memory_watchdog():
    while True:
        time.sleep(600)
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            if memory_mb > 450:
                logger.warning(f"⚠️ استهلاك الذاكرة: {memory_mb:.0f}MB - تنظيف احتياطي")
                with _global_state_lock:
                    if len(bot_stats.equity_curve) > 500:
                        bot_stats.equity_curve = deque(list(bot_stats.equity_curve)[-500:], maxlen=500)
                with _cache_lock:
                    for key in list(_ohlcv_cache.keys())[:30]: del _ohlcv_cache[key]
                with _features_cache_lock:
                    for key in list(_features_cache.keys())[:30]: del _features_cache[key]
                memory_after = process.memory_info().rss / 1024 / 1024
                logger.info(f"✅ بعد التنظيف: {memory_after:.0f}MB (وفر {memory_mb - memory_after:.0f}MB)")
                if memory_after > 1500:
                    logger.critical(f"❌ ذاكرة عالية جداً: {memory_after:.0f}MB - تنظيف إضافي")
                    send_telegram(f"🚨 ذاكرة عالية جداً ({memory_after:.0f}MB) - تنظيف إضافي")
        except ImportError: pass
        except Exception as e: logger.debug(f"خطأ في مراقبة الذاكرة: {e}")

def cooldown_cleanup():
    while True:
        time.sleep(60)
        now = datetime.now()
        with _cooldown_lock:
            expired = [sym for sym, end in _symbol_cooldown_until.items() if end <= now]
            for sym in expired:
                del _symbol_cooldown_until[sym]
                logger.debug(f"🧹 تم حذف تبريد {sym} (انتهت صلاحيته)")

def periodic_status_report():
    time.sleep(3600)
    while True:
        try:
            with _global_state_lock:
                positions_count = len(open_positions)
                paused = _is_paused()
                total_pnl = bot_stats.total_pnl_usdt
                daily_trades = _daily_trades_count
                total_equity = get_total_equity()
            report = (f"<b>📊 تقرير دوري (كل 4 ساعات)</b>\n🕒 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n💰 الرصيد الكلي: ${total_equity:.2f}\n📈 إجمالي الربح الصافي: ${total_pnl:.2f}\n📊 صفقات اليوم: {daily_trades}\n🔓 مراكز مفتوحة: {positions_count}\n⏸️ متوقف: {'نعم' if paused else 'لا'}\n🛑 عتبات الخسارة الوقائية:\n   - بيع 25% عند {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n   - بيع 33% عند {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n   - بيع كامل عند {STOP_LOSS_FULL_PERCENT:.2%}\n⚖️ أوزان الأطر: 5م={WEIGHT_5M:.2f}% | 15د={WEIGHT_15M:.2f}% | 1س={WEIGHT_1H:.2f}%\n🔺 <b>الحد الأعلى الموحد للثقة:</b> {UPPER_THRESHOLD_GLOBAL:.2f}\n📏 <b>حجم الصفقة (نسبة ثابتة):</b> {POSITION_SIZE_PERCENT:.1%}")
            if positions_count > 0:
                positions_detail = ""
                for sym, pos in open_positions.items():
                    ws_data = ws_manager.get_ticker(sym)
                    if ws_data and ws_data.get('price', 0) > 0:
                        price = ws_data['price']
                    else:
                        ticker = fetch_ticker_with_retry(sym, max_retries=1)
                        price = ticker['last'] if ticker else pos.entry_price
                    profit = (price - pos.entry_price) / pos.entry_price if pos.side == 'buy' else (pos.entry_price - price) / pos.entry_price
                    positions_detail += f"• {sym} ({pos.side}) ربح: {profit:.2%}\n"
                report += f"<b>📋 المراكز المفتوحة:</b>\n{positions_detail}"
            else: report += "لا توجد مراكز مفتوحة."
            send_telegram(report)
        except Exception as e: logger.error(f"خطأ في التقرير الدوري: {e}")
        time.sleep(14400)

# --------------------------- دالة telegram_polling المبسطة ---------------------------
def telegram_polling():
    global _last_telegram_update_id
    global FILTER_LIQUIDITY_ENABLED, FILTER_MARKET_CAP_ENABLED
    global FILTER_VOLUME_24H_ENABLED, FILTER_CHANGE_24H_ENABLED, FILTER_HOUR_CANDLE_ENABLED
    global STRENGTH_THRESHOLD, SCALP_MIN_PROFIT, SCAN_INTERVAL_MINUTES
    global PAPER_TRADING, TEST_MODE, bot_stats, open_positions
    global _daily_loss_tracker, _daily_trades_count, _daily_winning_trades, _daily_losing_trades
    global _daily_biggest_win, _daily_biggest_loss, _daily_most_traded
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    global STOP_LOSS_PARTIAL_1_PERCENT, STOP_LOSS_PARTIAL_2_PERCENT, STOP_LOSS_FULL_PERCENT
    global CUSTOM_ACTIVE_SYMBOLS_LIMIT, CUSTOM_MAX_EXPOSED_PERCENT
    global CUSTOM_MAX_DAILY_TRADES, CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP
    global buying_committee, CURRENT_BUY_COMMITTEE_MULTIPLIER
    global PAUSE_NEW_ENTRIES
    global COOLDOWN_WIN_HOURS, COOLDOWN_LOSS_MINUTES, _symbol_cooldown_until
    global USE_1H_MULTIPLIER
    global TRAILING_DISTANCE_PERCENT
    global PAUSE_ANALYSIS
    global WEIGHT_5M, WEIGHT_15M, WEIGHT_1H
    global SINGLE_MODEL_FILTER_MODEL, SINGLE_MODEL_FILTER_THRESHOLD, SINGLE_MODEL_FILTER_TIMEFRAME, SINGLE_MODEL_FILTER_ENABLED
    global SECOND_MODEL_FILTER_MODEL, SECOND_MODEL_FILTER_THRESHOLD, SECOND_MODEL_FILTER_TIMEFRAME, SECOND_MODEL_FILTER_ENABLED
    global THIRD_MODEL_FILTER_MODEL, THIRD_MODEL_FILTER_THRESHOLD, THIRD_MODEL_FILTER_TIMEFRAME, THIRD_MODEL_FILTER_ENABLED
    global FOURTH_MODEL_FILTER_MODEL, FOURTH_MODEL_FILTER_THRESHOLD, FOURTH_MODEL_FILTER_TIMEFRAME, FOURTH_MODEL_FILTER_ENABLED
    global FIFTH_MODEL_FILTER_MODEL, FIFTH_MODEL_FILTER_THRESHOLD, FIFTH_MODEL_FILTER_TIMEFRAME, FIFTH_MODEL_FILTER_ENABLED
    global SIXTH_MODEL_FILTER_MODEL, SIXTH_MODEL_FILTER_THRESHOLD, SIXTH_MODEL_FILTER_TIMEFRAME, SIXTH_MODEL_FILTER_ENABLED
    global SEVENTH_MODEL_FILTER_MODEL, SEVENTH_MODEL_FILTER_THRESHOLD, SEVENTH_MODEL_FILTER_TIMEFRAME, SEVENTH_MODEL_FILTER_ENABLED
    global UPPER_THRESHOLD_GLOBAL
    global POSITION_SIZE_PERCENT
    global _prev_active_symbols_limit
    global PAUSED, last_analysis_time

    logger.info("✅ بدء تشغيل مراقبة تلغرام...")
    load_telegram_last_id()
    while True:
        try:
            if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
                logger.warning("⚠️ تلغرام غير مهيأ: توكن أو معرف الدردشة مفقود")
                time.sleep(60)
                continue
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params = {"offset": _last_telegram_update_id+1, "timeout":30}
            try:
                resp = requests.get(url, params=params, timeout=35)
            except requests.exceptions.Timeout:
                logger.warning("مهلة طلب Telegram Update (تليغرام)")
                send_telegram("⚠️ إنذار: انتهت مهلة استقبال الرسائل، ولكن سيتم إعادة المحاولة تلقائياً.")
                time.sleep(10)
                continue
            if resp.status_code==200:
                data = resp.json()
                if data.get("ok"):
                    for upd in data.get("result",[]):
                        _last_telegram_update_id = upd["update_id"]
                        save_telegram_last_id()
                        msg = upd.get("message")
                        if msg and str(msg["chat"]["id"]) == str(TELEGRAM_CHAT_ID):
                            text = msg.get("text","").strip()
                            
                            # ========== الأوامر الأساسية ==========
                            if text == "توقف عن التداول":
                                with _state_lock:
                                    _set_paused(True)
                                close_all_positions("أمر تلغرام")
                                send_telegram("⏸️ تم الإيقاف وإغلاق الصفقات")
                                save_state()
                            elif text == "تابع التداول":
                                manual_resume()
                            elif text == "صفقاتي" or text == "صفقاتي":
                                with _global_state_lock:
                                    positions = list(open_positions.items())
                                if not positions:
                                    send_telegram("📭 لا توجد صفقات مفتوحة حالياً.")
                                else:
                                    total_value = 0.0; total_pnl = 0.0; lines = ["<b>📊 تقرير الصفقات المفتوحة</b>"]
                                    current_prices = {}
                                    for sym, _ in positions:
                                        ws_data = ws_manager.get_ticker(sym)
                                        if ws_data and ws_data.get('price', 0) > 0:
                                            current_prices[sym] = ws_data['price']
                                        else:
                                            ticker = fetch_ticker_with_retry(sym, max_retries=1)
                                            current_prices[sym] = ticker['last'] if ticker else None
                                    for sym, pos in positions:
                                        cur_price = current_prices.get(sym) or pos.entry_price
                                        if pos.side == 'buy':
                                            pnl = (cur_price - pos.entry_price) * pos.remaining_size
                                            pnl_percent = (cur_price / pos.entry_price - 1) * 100
                                        else:
                                            pnl = (pos.entry_price - cur_price) * pos.remaining_size
                                            pnl_percent = (1 - cur_price / pos.entry_price) * 100
                                        value = cur_price * pos.remaining_size
                                        total_value += value; total_pnl += pnl
                                        direction = "🟢" if pnl >= 0 else "🔴"
                                        lines.append(f"{direction} {sym} ({pos.side})\n   الكمية: {pos.remaining_size:.6f}\n   السعر الحالي: {cur_price:.8f}\n   سعر الدخول: {pos.entry_price:.8f}\n   الربح/الخسارة: {pnl:+.2f} USDT ({pnl_percent:+.2f}%)\n   القيمة: ${value:.2f}")
                                    lines.append(f"\n<b>📈 إجمالي قيمة المراكز: ${total_value:.2f}</b>")
                                    lines.append(f"<b>💰 إجمالي الربح/الخسارة العائم: {total_pnl:+.2f} USDT</b>")
                                    send_telegram("\n".join(lines))
                            elif text == 'رصيدي' or text == 'رصيدي':
                                try:
                                    if PAPER_TRADING or TEST_MODE:
                                        free_balance = bot_stats.last_balance
                                        mode_text = "ورقي (محاكاة)"
                                    else:
                                        real_bal = get_real_balance_usdt(max_retries=3, delay=2, silent=False)
                                        if real_bal is None:
                                            free_balance = bot_stats.last_balance
                                            mode_text = "حقيقي (بيانات مخزنة - قد لا تكون محدثة)"
                                        else:
                                            free_balance = real_bal
                                            mode_text = "حقيقي"
                                    positions_value = 0.0
                                    positions_details = []
                                    with _global_state_lock:
                                        for sym, pos in open_positions.items():
                                            ws_data = ws_manager.get_ticker(sym)
                                            if ws_data and ws_data.get('price', 0) > 0:
                                                current_price = ws_data['price']
                                            else:
                                                ticker = fetch_ticker_with_retry(sym, max_retries=1)
                                                current_price = ticker['last'] if ticker else pos.entry_price
                                            value = pos.remaining_size * current_price
                                            positions_value += value
                                            profit_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100 if pos.side == 'buy' else ((pos.entry_price - current_price) / pos.entry_price) * 100
                                            positions_details.append(f"• {sym} ({pos.side}) {pos.remaining_size:.6f} @ {current_price:.8f} ≈ ${value:.2f} (ربح {profit_pct:+.2f}%)")
                                    total = free_balance + positions_value
                                    msg = f"<b>💰 رصيدك الحالي ({mode_text})</b>\n━━━━━━━━━━━━━━━━━━━━\n💵 <b>الرصيد الحر:</b> ${free_balance:,.2f}\n"
                                    if positions_details:
                                        msg += f"\n📊 <b>المراكز المفتوحة ({len(positions_details)})</b>:\n" + "\n".join(positions_details) + f"\n\n💼 <b>قيمة المراكز:</b> ${positions_value:,.2f}\n"
                                    else:
                                        msg += f"\n📭 لا توجد مراكز مفتوحة.\n"
                                    msg += f"━━━━━━━━━━━━━━━━━━━━\n🏦 <b>الإجمالي الكلي:</b> ${total:,.2f}"
                                    send_telegram(msg)
                                except Exception as e:
                                    logger.error(f"خطأ في جلب الرصيد: {e}")
                                    send_telegram("⚠️ حدث خطأ أثناء جلب الرصيد. تأكد من الاتصال بالمنصة.")
                            elif text == 'عتبات الخسارة':
                                send_telegram(f"🛑 عتبات الخسارة الوقائية الحالية:\n- بيع 25% عند خسارة {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n- بيع 33% عند خسارة {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n- بيع كامل عند خسارة {STOP_LOSS_FULL_PERCENT:.2%}")
                            
                            # ========== حالة الفلاتر ==========
                            elif text == 'حالة الفلاتر':
                                w = buying_committee.weights
                                status = (f"📊 <b>حالة الفلاتر والقيم الحالية</b>\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"💧 <b>فلتر السيولة:</b> {'✅ مفعل' if FILTER_LIQUIDITY_ENABLED else '❌ معطل'}\n"
                                          f"💰 <b>فلتر القيمة السوقية:</b> {'✅ مفعل' if FILTER_MARKET_CAP_ENABLED else '❌ معطل'} (حد: ${MIN_MARKET_CAP_USD:,.0f})\n"
                                          f"📊 <b>فلتر حجم التداول 24ساعة:</b> {'✅ مفعل' if FILTER_VOLUME_24H_ENABLED else '❌ معطل'} (حد: ${MIN_24H_VOLUME_USD:,.0f})\n"
                                          f"📈 <b>فلتر التغيير 24ساعة:</b> {'✅ مفعل' if FILTER_CHANGE_24H_ENABLED else '❌ معطل'} (حد: {MIN_24H_CHANGE_PERCENT:.1f}%)\n"
                                          f"🕐 <b>فلتر شمعة الساعة:</b> {'✅ مفعل' if FILTER_HOUR_CANDLE_ENABLED else '❌ معطل'}\n"
                                          f"⏸️ <b>دخول الصفقات الجديدة:</b> {'🟢 مفعل (مسموح)' if not PAUSE_NEW_ENTRIES else '🔴 موقف (ممنوع)'}\n"
                                          f"🔁 <b>المضاعف (من 5 دقائق):</b> {'✅ مفعل' if USE_1H_MULTIPLIER else '❌ معطل'}\n"
                                          f"📊 <b>نسبة تتبع الأرباح (الوقف المتحرك):</b> {TRAILING_DISTANCE_PERCENT:.2%}\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"⚖️ <b>أوزان الأطر الحالية:</b> 5م={WEIGHT_5M:.2f}% | 15د={WEIGHT_15M:.2f}% | 1س={WEIGHT_1H:.2f}%\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"⚖️ <b>أوزان النماذج (لجنة الشراء):</b>\n"
                                          f"   Rule: {w['rule']*100:.1f}%\n"
                                          f"   EMR: {w['emr']*100:.1f}%\n"
                                          f"   CFHM: {w['cfhm']*100:.1f}%\n"
                                          f"   Timing: {w['timing']*100:.1f}%\n"
                                          f"   VWAP_OBV: {w['vwap_obv']*100:.1f}%\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"⚙️ <b>الإعدادات الديناميكية:</b>\n"
                                          f"🔢 <b>عدد رموز التحليل:</b> {CUSTOM_ACTIVE_SYMBOLS_LIMIT}\n"
                                          f"📊 <b>عدد الصفقات المسموحة يومياً:</b> {CUSTOM_MAX_DAILY_TRADES}\n"
                                          f"📉 <b>الحد الأقصى للتعرض الكلي:</b> {CUSTOM_MAX_EXPOSED_PERCENT:.1%}\n"
                                          f"📏 <b>حجم الصفقة (نسبة ثابتة):</b> {POSITION_SIZE_PERCENT:.1%}\n"
                                          f"⏱️ <b>فترة الانتظار قبل البيع الكامل:</b> {CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP} دقيقة\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"⚙️ <b>عتبات لجنة الشراء:</b> مضاعف {CURRENT_BUY_COMMITTEE_MULTIPLIER:.2f}×\n"
                                          f"🎯 <b>عتبة شراء عادي (الحد الأدنى):</b> {STRENGTH_THRESHOLD:.2f}\n"
                                          f"🎯 <b>عتبة شراء ميم (الحد الأدنى):</b> {SCALP_MIN_PROFIT:.2f}\n"
                                          f"🔺 <b>الحد الأعلى الموحد للثقة:</b> {UPPER_THRESHOLD_GLOBAL:.2f}\n"
                                          f"🛑 <b>عتبات الخسارة الوقائية:</b>\n"
                                          f"   - بيع 25% عند {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n"
                                          f"   - بيع 33% عند {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n"
                                          f"   - بيع كامل عند {STOP_LOSS_FULL_PERCENT:.2%}\n"
                                          f"🔄 <b>مدة المسح:</b> {SCAN_INTERVAL_MINUTES} دقيقة\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"⏳ <b>نظام التبريد الذكي:</b>\n"
                                          f"   ✅ تبريد الصفقة الرابحة: {COOLDOWN_WIN_HOURS:.1f} ساعة\n"
                                          f"   🔴 تبريد الصفقة الخاسرة: {COOLDOWN_LOSS_MINUTES:.0f} دقائق\n"
                                          f"🎯 <b>فلتر النموذج الأول (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {SINGLE_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {SINGLE_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {SINGLE_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if SINGLE_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج الثاني (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {SECOND_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {SECOND_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {SECOND_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if SECOND_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج الثالث (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {THIRD_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {THIRD_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {THIRD_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if THIRD_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج الرابع (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {FOURTH_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {FOURTH_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {FOURTH_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if FOURTH_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج الخامس (حد أعلى):</b>\n"
                                          f"   - النموذج المختار: {FIFTH_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {FIFTH_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {FIFTH_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if FIFTH_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج السادس (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {SIXTH_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {SIXTH_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {SIXTH_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if SIXTH_MODEL_FILTER_ENABLED else '❌ معطل'}\n"
                                          f"🎯 <b>فلتر النموذج السابع (حد أدنى):</b>\n"
                                          f"   - النموذج المختار: {SEVENTH_MODEL_FILTER_MODEL}\n"
                                          f"   - العتبة: {SEVENTH_MODEL_FILTER_THRESHOLD:.2f}\n"
                                          f"   - الإطار الزمني: {SEVENTH_MODEL_FILTER_TIMEFRAME}\n"
                                          f"   - الحالة: {'✅ مفعل' if SEVENTH_MODEL_FILTER_ENABLED else '❌ معطل'}")
                                send_telegram(status)
                            
                            # ========== أوامر تشغيل/إيقاف الفلاتر ==========
                            elif text == 'اوقف فلتر السيولة':
                                FILTER_LIQUIDITY_ENABLED = False
                                send_telegram("🔴 تم إيقاف فلتر السيولة (لن يتم فحص العمق والسبريد)")
                                save_state()
                            elif text == 'شغل فلتر السيولة':
                                FILTER_LIQUIDITY_ENABLED = True
                                send_telegram("🟢 تم تشغيل فلتر السيولة (سيتم فحص العمق والسبريد قبل الشراء)")
                                save_state()
                            elif text == 'اوقف فلتر القيمة السوقية':
                                FILTER_MARKET_CAP_ENABLED = False
                                send_telegram("🔴 تم إيقاف فلتر القيمة السوقية (لن يتم فحص الحد الأدنى للقيمة السوقية)")
                                save_state()
                            elif text == 'شغل فلتر القيمة السوقية':
                                FILTER_MARKET_CAP_ENABLED = True
                                send_telegram("🟢 تم تشغيل فلتر القيمة السوقية (سيتم فحص MIN_MARKET_CAP_USD)")
                                save_state()
                            elif text == 'اوقف فلتر حجم التداول 24ساعة':
                                FILTER_VOLUME_24H_ENABLED = False
                                send_telegram("🔴 تم إيقاف فلتر حجم التداول 24 ساعة (لن يتم فحص MIN_24H_VOLUME_USD)")
                                save_state()
                            elif text == 'شغل فلتر حجم التداول 24ساعة':
                                FILTER_VOLUME_24H_ENABLED = True
                                send_telegram("🟢 تم تشغيل فلتر حجم التداول 24 ساعة (سيتم فحص الحجم)")
                                save_state()
                            elif text == 'اوقف فلتر التغيير 24ساعة':
                                FILTER_CHANGE_24H_ENABLED = False
                                send_telegram("🔴 تم إيقاف فلتر التغيير 24 ساعة (لن يتم فحص MIN_24H_CHANGE_PERCENT)")
                                save_state()
                            elif text == 'شغل فلتر التغيير 24ساعة':
                                FILTER_CHANGE_24H_ENABLED = True
                                send_telegram("🟢 تم تشغيل فلتر التغيير 24 ساعة (سيتم فحص التغير)")
                                save_state()
                            elif text == 'اوقف فلتر الشمعة الصاعدة':
                                FILTER_HOUR_CANDLE_ENABLED = False
                                send_telegram("🔴 تم إيقاف فلتر شمعة الساعة الصاعدة (لن يتم فحص السعر > إغلاق آخر ساعة)")
                                save_state()
                            elif text == 'شغل فلتر الشمعة الصاعدة':
                                FILTER_HOUR_CANDLE_ENABLED = True
                                send_telegram("🟢 تم تشغيل فلتر شمعة الساعة الصاعدة (سيتم فحص السعر > إغلاق آخر ساعة)")
                                save_state()
                            
                            # ========== أوامر ضبط العتبات ==========
                            elif text.startswith('ارفع عتبة لجنة الشراء'):
                                match = re.search(r'(\d+(?:\.\d+)?)%', text)
                                if match:
                                    percent = float(match.group(1))
                                    new_multiplier = CURRENT_BUY_COMMITTEE_MULTIPLIER * (1 + percent / 100.0)
                                    new_multiplier = max(0.5, min(2.0, new_multiplier))
                                    buying_committee.apply_multiplier(new_multiplier)
                                    send_telegram(f"✅ تم رفع عتبات لجنة الشراء بنسبة {percent}%\n📊 المضاعف الجديد: {new_multiplier:.2f} (أي عتبات ×{new_multiplier:.2f})")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `ارفع عتبة لجنة الشراء 20%`")
                            elif text.startswith('خفض عتبة لجنة الشراء'):
                                match = re.search(r'(\d+(?:\.\d+)?)%', text)
                                if match:
                                    percent = float(match.group(1))
                                    new_multiplier = CURRENT_BUY_COMMITTEE_MULTIPLIER * (1 - percent / 100.0)
                                    new_multiplier = max(0.5, min(2.0, new_multiplier))
                                    buying_committee.apply_multiplier(new_multiplier)
                                    send_telegram(f"✅ تم خفض عتبات لجنة الشراء بنسبة {percent}%\n📊 المضاعف الجديد: {new_multiplier:.2f} (أي عتبات ×{new_multiplier:.2f})")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `خفض عتبة لجنة الشراء 10%`")
                            elif text.startswith('عتبة شراء ميم'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_val = float(match.group(1))
                                    if 0.05 <= new_val <= 0.90:
                                        SCALP_MIN_PROFIT = new_val
                                        send_telegram(f"✅ تم تغيير عتبة شراء العملات الميم إلى {new_val:.2f}")
                                        save_state()
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.90")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عتبة شراء ميم 0.25`")
                            elif text.startswith('عتبة شراء عادية'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_val = float(match.group(1))
                                    if 0.05 <= new_val <= 0.90:
                                        STRENGTH_THRESHOLD = new_val
                                        send_telegram(f"✅ تم تغيير عتبة شراء العملات العادية إلى {new_val:.2f}")
                                        save_state()
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.90")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عتبة شراء عادية 0.30`")
                            
                            # ========== أوامر ضبط مدة المسح ووقت الانتظار ==========
                            elif text.startswith('امسح السوق كل'):
                                match = re.search(r'(\d+)\s*دقيقة', text)
                                if match:
                                    new_interval = int(match.group(1))
                                    if 1 <= new_interval <= 1440:
                                        SCAN_INTERVAL_MINUTES = new_interval
                                        if new_interval >= 60:
                                            hours = new_interval / 60
                                            send_telegram(f"✅ تم تغيير مدة المسح إلى {new_interval} دقيقة (أي {hours:.1f} ساعة)")
                                        else:
                                            send_telegram(f"✅ تم تغيير مدة المسح إلى {new_interval} دقيقة")
                                        save_state()
                                    else:
                                        send_telegram("⚠️ يرجى إدخال قيمة بين 1 و 1440 دقيقة (أي بين دقيقة واحدة و 24 ساعة)")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `امسح السوق كل 5 دقيقة`")
                            elif text.startswith('فترة الانتظار'):
                                match = re.search(r'(\d+)\s*دقيقة', text)
                                if match:
                                    new_minutes = int(match.group(1))
                                    if 5 <= new_minutes <= 300:
                                        CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP = new_minutes
                                        save_state()
                                        send_telegram(f"✅ تم تعيين فترة الانتظار قبل البيع الكامل إلى {new_minutes} دقيقة.")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 5 و 300 دقيقة.")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `فترة الانتظار 90 دقيقة`")
                            
                            # ========== أوامر التبديل بين التداول الحقيقي والورقي ==========
                            elif text == 'تداول حقيقي':
                                if not ENABLE_TRADING:
                                    send_telegram("⚠️ التداول غير مفعل في الإعدادات الأساسية (ENABLE_TRADING=false). لا يمكن التحويل.")
                                elif PAPER_TRADING:
                                    with _global_state_lock:
                                        if open_positions:
                                            send_telegram("🔄 جاري إغلاق جميع المراكز الورقية قبل التحويل...")
                                    close_all_positions("التحويل إلى التداول الحقيقي - إغلاق المراكز الورقية")
                                    time.sleep(2)
                                    with _global_state_lock:
                                        open_positions.clear()
                                        _daily_loss_tracker = 0.0
                                        _daily_trades_count = 0
                                        _daily_winning_trades = 0
                                        _daily_losing_trades = 0
                                        _daily_biggest_win = 0.0
                                        _daily_biggest_loss = 0.0
                                        _daily_most_traded.clear()
                                        _daily_total_holding_time_win = 0.0
                                        _daily_total_holding_time_loss = 0.0
                                        _daily_holding_count_win = 0
                                        _daily_holding_count_loss = 0
                                        bot_stats.total_pnl_usdt = 0.0
                                        bot_stats.winning_trades = 0
                                        bot_stats.losing_trades = 0
                                        bot_stats.total_trades = 0
                                        bot_stats.daily_pnl = 0.0
                                        bot_stats.weekly_pnl = 0.0
                                        bot_stats.weekly_wins = 0
                                        bot_stats.weekly_losses = 0
                                    PAPER_TRADING = False
                                    TEST_MODE = False
                                    send_telegram("⏳ جاري الاتصال بـ Binance لجلب الرصيد الحقيقي (قد يستغرق حتى دقيقتين)...")
                                    real_balance = fetch_real_balance_with_retry(timeout_seconds=120, retry_interval=5, silent=False)
                                    if real_balance is not None:
                                        with _global_state_lock:
                                            bot_stats.last_balance = real_balance
                                        send_telegram(f"✅ تم التبديل إلى **التداول الحقيقي**. الرصيد الحقيقي: ${real_balance:.2f} USDT")
                                        save_state()
                                    else:
                                        PAPER_TRADING = True
                                        send_telegram("❌ فشل جلب الرصيد الحقيقي من Binance. تأكد من اتصال API والمفاتيح.\n⚠️ لم يتم التبديل إلى التداول الحقيقي. بقيت في **الوضع الورقي**.")
                                else:
                                    send_telegram("ℹ️ أنت بالفعل في وضع التداول الحقيقي.")
                            elif text == 'تداول ورقي':
                                if not ENABLE_TRADING:
                                    send_telegram("⚠️ التداول غير مفعل في الإعدادات الأساسية (ENABLE_TRADING=false). لا يمكن التحويل.")
                                elif not PAPER_TRADING and not TEST_MODE:
                                    if open_positions:
                                        send_telegram("🔄 جاري إغلاق جميع المراكز الحقيقية قبل التحويل إلى التداول الورقي...")
                                    close_all_positions("التحويل إلى التداول الورقي - إغلاق المراكز الحقيقية")
                                    time.sleep(2)
                                    with _global_state_lock:
                                        open_positions.clear()
                                        _daily_loss_tracker = 0.0
                                        _daily_trades_count = 0
                                        _daily_winning_trades = 0
                                        _daily_losing_trades = 0
                                        _daily_biggest_win = 0.0
                                        _daily_biggest_loss = 0.0
                                        _daily_most_traded.clear()
                                        _daily_total_holding_time_win = 0.0
                                        _daily_total_holding_time_loss = 0.0
                                        _daily_holding_count_win = 0
                                        _daily_holding_count_loss = 0
                                        bot_stats.total_pnl_usdt = 0.0
                                        bot_stats.winning_trades = 0
                                        bot_stats.losing_trades = 0
                                        bot_stats.total_trades = 0
                                        bot_stats.daily_pnl = 0.0
                                        bot_stats.weekly_pnl = 0.0
                                        bot_stats.weekly_wins = 0
                                        bot_stats.weekly_losses = 0
                                        bot_stats.last_balance = PAPER_INITIAL_BALANCE
                                        bot_stats.equity_curve = deque(maxlen=1000)
                                        bot_stats.symbol_performance = {}
                                        _daily_trades_date = datetime.now().date()
                                    PAPER_TRADING = True
                                    TEST_MODE = False
                                    send_telegram(f"✅ تم التبديل إلى **التداول الورقي** برصيد ابتدائي ${PAPER_INITIAL_BALANCE:.2f} USDT.")
                                    save_state()
                                else:
                                    send_telegram("ℹ️ أنت بالفعل في وضع التداول الورقي أو المحاكاة.")
                            
                            # ========== أوامر تعيين عتبات الخسارة الوقائية ==========
                            elif text.startswith('تعيين عتبة خسارة 25%'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?\s*$', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.001 <= new_percent <= 0.05:
                                        STOP_LOSS_PARTIAL_1_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة الخسارة (بيع 25%) إلى {new_percent:.2%}\n(سيتم بيع 25% عند خسارة {new_percent:.2%})")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.1% و 5%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة خسارة 25% 0.9`")
                            elif text.startswith('تعيين عتبة خسارة 33%'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?\s*$', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.001 <= new_percent <= 0.05:
                                        STOP_LOSS_PARTIAL_2_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة الخسارة (بيع 33%) إلى {new_percent:.2%}\n(سيتم بيع 33% عند خسارة {new_percent:.2%})")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.1% و 5%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة خسارة 33% 1.2`")
                            elif text.startswith('تعيين عتبة خسارة 100%'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?\s*$', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.001 <= new_percent <= 0.10:
                                        STOP_LOSS_FULL_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة الخسارة الكاملة إلى {new_percent:.2%}\n(سيتم بيع كامل المركز عند خسارة {new_percent:.2%})")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.1% و 10%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة خسارة 100% 1.72`")
                            
                            # ========== الأوامر الديناميكية ==========
                            elif text.startswith('عدد رموز التحليل'):
                                match = re.search(r'(\d+)', text)
                                if match:
                                    new_limit = int(match.group(1))
                                    if 0 <= new_limit <= 200:
                                        old_limit = CUSTOM_ACTIVE_SYMBOLS_LIMIT
                                        CUSTOM_ACTIVE_SYMBOLS_LIMIT = new_limit
                                        if old_limit == 0 and new_limit > 0:
                                            if scanner:
                                                scanner.last_scan = 0
                                                scanner.next_scan_time = 0
                                            force_unlock()
                                            with _last_analysis_time_lock:
                                                last_analysis_time = datetime.now() - timedelta(minutes=5)
                                            PAUSED = False
                                            PAUSE_ANALYSIS = False
                                            PAUSE_NEW_ENTRIES = False
                                            _prev_active_symbols_limit = new_limit
                                            send_telegram("🔄 تم إعادة ضبط المسح والتحليل بعد الخروج من حالة الصفر.")
                                        _prev_active_symbols_limit = new_limit
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عدد الرموز للتحليل إلى {new_limit}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0 و 200")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عدد رموز التحليل 30`")
                            elif text.startswith('حجم التعرض'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.1 <= new_percent <= 1.0:
                                        CUSTOM_MAX_EXPOSED_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين الحد الأقصى للتعرض الكلي إلى {new_percent:.1%}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 10% و 100%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `حجم التعرض 65%`")
                            elif text.startswith('عدد الصفقات المسموحة') or text.startswith('حد الصفقات اليومي'):
                                match = re.search(r'(\d+)', text)
                                if match:
                                    new_limit = int(match.group(1))
                                    if 1 <= new_limit <= 200:
                                        CUSTOM_MAX_DAILY_TRADES = new_limit
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عدد الصفقات المسموحة يومياً إلى {new_limit}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 1 و 200")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عدد الصفقات المسموحة 40`")
                            
                            # ========== أوامر التبريد ==========
                            elif text.startswith('تعيين تبريد الرابح'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*(ساعة|دقيقة)', text)
                                if match:
                                    value = float(match.group(1))
                                    unit = match.group(2)
                                    hours = value / 60.0 if unit == 'دقيقة' else value
                                    if 0.5 <= hours <= 72:
                                        COOLDOWN_WIN_HOURS = hours
                                        save_state()
                                        send_telegram(f"✅ تم تعيين تبريد الصفقات الرابحة إلى {value} {unit} (أي {hours:.1f} ساعة)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.5 ساعة و 72 ساعة")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين تبريد الرابح 10 ساعة` أو `تعيين تبريد الرابح 600 دقيقة`")
                            elif text.startswith('تعيين تبريد الخاسر'):
                                match = re.search(r'(\d+)\s*دقيقة', text)
                                if match:
                                    minutes = int(match.group(1))
                                    if 1 <= minutes <= 120:
                                        COOLDOWN_LOSS_MINUTES = float(minutes)
                                        save_state()
                                        send_telegram(f"✅ تم تعيين تبريد الصفقات الخاسرة إلى {minutes} دقيقة")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 1 و 120 دقيقة")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين تبريد الخاسر 7 دقيقة`")
                            elif text == 'عرض التبريد':
                                with _cooldown_lock:
                                    if not _symbol_cooldown_until:
                                        send_telegram("📭 لا توجد أي عملات في فترة تبريد حالياً.")
                                    else:
                                        lines = ["<b>⏳ العملات في فترة التبريد</b>"]
                                        now = datetime.now()
                                        for sym, end_time in _symbol_cooldown_until.items():
                                            remaining = (end_time - now).total_seconds()
                                            if remaining > 0:
                                                hours = remaining / 3600
                                                lines.append(f"• {sym}: {hours:.1f} ساعة متبقية ({remaining/60:.0f} دقيقة)")
                                            else:
                                                lines.append(f"• {sym}: انتهت (سيتم حذفها قريباً)")
                                        send_telegram("\n".join(lines))
                            elif text == 'تصفير التبريد':
                                with _cooldown_lock:
                                    _symbol_cooldown_until.clear()
                                send_telegram("🧹 تم تصفير التبريد (جميع الرموز أصبحت جاهزة للشراء).")
                            
                            # ========== أوامر إيقاف/تشغيل التحليل ==========
                            elif text == 'إيقاف التحليل':
                                PAUSE_ANALYSIS = True
                                send_telegram("⏸️ تم إيقاف التحليل ومسح السوق. المراكز المفتوحة لا تزال تحت المراقبة.")
                            elif text == 'تشغيل التحليل':
                                PAUSE_ANALYSIS = False
                                send_telegram("▶️ تم تشغيل التحليل ومسح السوق.")
                            
                            # ========== أوامر الفلاتر السبعة ==========
                            # فلتر الأول
                            elif text.startswith('تعيين نموذج الفلتر الأول'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        SINGLE_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر الأول إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر الأول rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر الأول'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        SINGLE_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر الأول إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر الأول 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر الأول'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    SINGLE_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر الأول إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر الأول 15m`")
                            elif text == 'شغل فلتر النموذج الأول':
                                SINGLE_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج الأول (النموذج: {SINGLE_MODEL_FILTER_MODEL}, الإطار: {SINGLE_MODEL_FILTER_TIMEFRAME}, العتبة: {SINGLE_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج الأول':
                                SINGLE_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج الأول (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر الثاني
                            elif text.startswith('تعيين نموذج الفلتر الثاني'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        SECOND_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر الثاني إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر الثاني rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر الثاني'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        SECOND_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر الثاني إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر الثاني 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر الثاني'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    SECOND_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر الثاني إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر الثاني 15m`")
                            elif text == 'شغل فلتر النموذج الثاني':
                                SECOND_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج الثاني (النموذج: {SECOND_MODEL_FILTER_MODEL}, الإطار: {SECOND_MODEL_FILTER_TIMEFRAME}, العتبة: {SECOND_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج الثاني':
                                SECOND_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج الثاني (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر الثالث
                            elif text.startswith('تعيين نموذج الفلتر الثالث'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        THIRD_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر الثالث إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر الثالث rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر الثالث'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        THIRD_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر الثالث إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر الثالث 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر الثالث'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    THIRD_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر الثالث إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر الثالث 15m`")
                            elif text == 'شغل فلتر النموذج الثالث':
                                THIRD_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج الثالث (النموذج: {THIRD_MODEL_FILTER_MODEL}, الإطار: {THIRD_MODEL_FILTER_TIMEFRAME}, العتبة: {THIRD_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج الثالث':
                                THIRD_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج الثالث (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر الرابع
                            elif text.startswith('تعيين نموذج الفلتر الرابع'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        FOURTH_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر الرابع إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر الرابع rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر الرابع'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        FOURTH_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر الرابع إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر الرابع 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر الرابع'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    FOURTH_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر الرابع إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر الرابع 15m`")
                            elif text == 'شغل فلتر النموذج الرابع':
                                FOURTH_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج الرابع (النموذج: {FOURTH_MODEL_FILTER_MODEL}, الإطار: {FOURTH_MODEL_FILTER_TIMEFRAME}, العتبة: {FOURTH_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج الرابع':
                                FOURTH_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج الرابع (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر الخامس
                            elif text.startswith('تعيين نموذج الفلتر الخامس'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        FIFTH_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر الخامس إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر الخامس rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر الخامس'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        FIFTH_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر الخامس إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر الخامس 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر الخامس'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    FIFTH_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر الخامس إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر الخامس 15m`")
                            elif text == 'شغل فلتر النموذج الخامس':
                                FIFTH_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج الخامس (النموذج: {FIFTH_MODEL_FILTER_MODEL}, الإطار: {FIFTH_MODEL_FILTER_TIMEFRAME}, العتبة: {FIFTH_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج الخامس':
                                FIFTH_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج الخامس (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر السادس
                            elif text.startswith('تعيين نموذج الفلتر السادس'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        SIXTH_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر السادس إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر السادس rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر السادس'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        SIXTH_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر السادس إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر السادس 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر السادس'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    SIXTH_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر السادس إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر السادس 15m`")
                            elif text == 'شغل فلتر النموذج السادس':
                                SIXTH_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج السادس (النموذج: {SIXTH_MODEL_FILTER_MODEL}, الإطار: {SIXTH_MODEL_FILTER_TIMEFRAME}, العتبة: {SIXTH_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج السادس':
                                SIXTH_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج السادس (لن يُطبق أي فلتر إضافي)")
                            
                            # فلتر السابع
                            elif text.startswith('تعيين نموذج الفلتر السابع'):
                                match = re.search(r'(rule|emr|cfhm|timing|vwap_obv|vwap)', text.lower())
                                if match:
                                    model = match.group(1)
                                    if model == 'vwap':
                                        model = 'vwap_obv'
                                    if model in ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']:
                                        SEVENTH_MODEL_FILTER_MODEL = model
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نموذج الفلتر السابع إلى {model}")
                                    else:
                                        send_telegram("⚠️ النموذج غير معروف. اختر: rule, emr, cfhm, timing, vwap_obv")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين نموذج الفلتر السابع rule`")
                            elif text.startswith('تعيين عتبة نموذج الفلتر السابع'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_threshold = float(match.group(1))
                                    if 0.05 <= new_threshold <= 0.95:
                                        SEVENTH_MODEL_FILTER_THRESHOLD = new_threshold
                                        save_state()
                                        send_telegram(f"✅ تم تعيين عتبة نموذج الفلتر السابع إلى {new_threshold:.2f}")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين عتبة نموذج الفلتر السابع 0.60`")
                            elif text.startswith('تعيين إطار نموذج الفلتر السابع'):
                                match = re.search(r'(5m|15m|1h)', text)
                                if match:
                                    tf = match.group(1)
                                    SEVENTH_MODEL_FILTER_TIMEFRAME = tf
                                    save_state()
                                    send_telegram(f"✅ تم تعيين إطار نموذج الفلتر السابع إلى {tf}")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين إطار نموذج الفلتر السابع 15m`")
                            elif text == 'شغل فلتر النموذج السابع':
                                SEVENTH_MODEL_FILTER_ENABLED = True
                                save_state()
                                send_telegram(f"🟢 تم تفعيل فلتر النموذج السابع (النموذج: {SEVENTH_MODEL_FILTER_MODEL}, الإطار: {SEVENTH_MODEL_FILTER_TIMEFRAME}, العتبة: {SEVENTH_MODEL_FILTER_THRESHOLD:.2f})")
                            elif text == 'أوقف فلتر النموذج السابع':
                                SEVENTH_MODEL_FILTER_ENABLED = False
                                save_state()
                                send_telegram("🔴 تم إيقاف فلتر النموذج السابع (لن يُطبق أي فلتر إضافي)")
                            
                            # ========== أوامر تتبع الأرباح ==========
                            elif text.startswith('تتبع الأرباح عند نسبة'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.001 <= new_percent <= 0.10:
                                        TRAILING_DISTANCE_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين نسبة تتبع الأرباح إلى {new_percent:.2%}")
                                    else:
                                        send_telegram("⚠️ النسبة يجب أن تكون بين 0.1% و 10%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تتبع الأرباح عند نسبة 0.99%`")
                            
                            # ========== أوامر الأوزان ==========
                            elif text.startswith('تعيين أوزان الأطر'):
                                parts = text.split()
                                if len(parts) < 4:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `تعيين أوزان الأطر 30 60 10` (المجموع يجب أن يساوي 100)")
                                    continue
                                try:
                                    w5 = float(parts[-1])
                                    w15 = float(parts[-2])
                                    w1h = float(parts[-3])
                                except ValueError:
                                    send_telegram("⚠️ القيم يجب أن تكون أرقاماً. استخدم: `تعيين أوزان الأطر 30 60 10`")
                                    continue
                                if w5 <= 0 or w15 <= 0 or w1h <= 0:
                                    send_telegram("⚠️ جميع الأوزان يجب أن تكون أكبر من 0.")
                                    continue
                                total = w5 + w15 + w1h
                                if abs(total - 100.0) > 0.01:
                                    send_telegram(f"⚠️ مجموع الأوزان = {total:.2f}% يجب أن يساوي 100% بالضبط. تأكد من جمعها 100.")
                                    continue
                                WEIGHT_5M = w5
                                WEIGHT_15M = w15
                                WEIGHT_1H = w1h
                                save_state()
                                send_telegram(f"✅ تم تعيين أوزان الأطر:\n📊 5 دقائق: {w5:.2f}%\n📊 15 دقيقة: {w15:.2f}%\n📊 1 ساعة: {w1h:.2f}%")
                            
                            elif text.startswith('تعيين أوزان النماذج') or text.startswith('ضبط أوزان اللجنة'):
                                pattern = re.compile(r'(rule|emr|cfhm|timing|vwap_obv|vwap)\s*[:=]\s*(\d+(?:\.\d+)?)|(rule|emr|cfhm|timing|vwap_obv|vwap)\s+(\d+(?:\.\d+)?)', re.IGNORECASE)
                                matches = pattern.findall(text)
                                if not matches:
                                    send_telegram("⚠️ لم يتم العثور على أسماء وقيم صحيحة.\nالصيغة المطلوبة: `تعيين أوزان النماذج Rule=10 EMR=5 CFHM=10 Timing=55 VWAP=20`")
                                    continue
                                new_weights = {}
                                for m in matches:
                                    if m[0]:
                                        name = m[0].lower()
                                        val = float(m[1])
                                    else:
                                        name = m[2].lower()
                                        val = float(m[3])
                                    if name == 'vwap':
                                        name = 'vwap_obv'
                                    new_weights[name] = val / 100.0
                                required = ['rule', 'emr', 'cfhm', 'timing', 'vwap_obv']
                                if not all(k in new_weights for k in required):
                                    missing = [k for k in required if k not in new_weights]
                                    send_telegram(f"⚠️ القيم المطلوبة: {', '.join(missing)} لم تُرسل. أعد المحاولة.")
                                    continue
                                total = sum(new_weights.values()) * 100
                                if abs(total - 100.0) > 0.01:
                                    send_telegram(f"⚠️ مجموع الأوزان = {total:.2f}% يجب أن يساوي 100%. تأكد من القيم.")
                                    continue
                                buying_committee.weights = new_weights
                                save_state()
                                msg = (f"✅ تم تحديث أوزان النماذج بنجاح:\n"
                                       f"• Rule: {new_weights['rule']*100:.1f}%\n"
                                       f"• EMR: {new_weights['emr']*100:.1f}%\n"
                                       f"• CFHM: {new_weights['cfhm']*100:.1f}%\n"
                                       f"• Timing: {new_weights['timing']*100:.1f}%\n"
                                       f"• VWAP_OBV: {new_weights['vwap_obv']*100:.1f}%")
                                send_telegram(msg)
                            elif text == 'عرض أوزان النماذج' or text == 'أوزان اللجنة':
                                w = buying_committee.weights
                                msg = (f"📊 الأوزان الحالية للنماذج:\n"
                                       f"🔹 Rule: {w['rule']*100:.1f}%\n"
                                       f"🔹 EMR: {w['emr']*100:.1f}%\n"
                                       f"🔹 CFHM: {w['cfhm']*100:.1f}%\n"
                                       f"🔹 Timing: {w['timing']*100:.1f}%\n"
                                       f"🔹 VWAP_OBV: {w['vwap_obv']*100:.1f}%")
                                send_telegram(msg)
                            
                            # ========== أمر حجم الصفقة ==========
                            elif text.startswith('حجم الصفقة'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.01 <= new_percent <= 1.0:
                                        POSITION_SIZE_PERCENT = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم تعيين حجم الصفقة إلى {new_percent:.1%}")
                                    else:
                                        send_telegram("⚠️ النسبة يجب أن تكون بين 1% و 100%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `حجم الصفقة 25%`")
                            
                            # ========== أوامر المضاعف ==========
                            elif text == 'شغل المضاعف':
                                USE_1H_MULTIPLIER = True
                                save_state()
                                send_telegram("✅ تم **تفعيل** المضاعف المستند إلى إطار 5 دقائق.\nسيتم ضرب درجة الثقة النهائية بالمضاعف المستخلص من تحليل الإطار قصير المدى.")
                            elif text == 'أوقف المضاعف':
                                USE_1H_MULTIPLIER = False
                                save_state()
                                send_telegram("⛔ تم **إيقاف** المضاعف المستند إلى إطار 5 دقائق.\nستُستخدم درجة الثقة النهائية كما هي من الإطار الأساسي (15 دقيقة) دون أي تعديل.")
                            
                            # ========== الحد الأعلى للثقة ==========
                            elif text.startswith('الحد الأعلى للثقة'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_val = float(match.group(1))
                                    if 0.05 <= new_val <= 0.95:
                                        UPPER_THRESHOLD_GLOBAL = new_val
                                        save_state()
                                        send_telegram(f"✅ تم تعيين **الحد الأعلى الموحد** للثقة إلى {new_val:.2f} (سيتم رفض الصفقات التي تتجاوز هذه القيمة)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.95")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `الحد الأعلى للثقة 0.75`")
                            
                            # ========== أوامر إضافية ==========
                            elif text == 'توقف عن دخول الصفقات':
                                PAUSE_NEW_ENTRIES = True
                                save_state()
                                send_telegram("⏸️ تم إيقاف دخول الصفقات الجديدة. لن يتم فتح أي صفقة جديدة حتى إشعار آخر.\n✅ المراكز المفتوحة لا تزال تحت المراقبة والبيع العادي.")
                            elif text == 'تابع دخول الصفقات':
                                PAUSE_NEW_ENTRIES = False
                                save_state()
                                send_telegram("▶️ تم استئناف دخول الصفقات الجديدة. البوت سيفتح صفقات جديدة حسب الخوارزمية.")
                            
                            # ========== أمر إعادة الضبط ==========
                            elif text == 'اعادة ضبط الفلاتر':
                                FILTER_LIQUIDITY_ENABLED = True
                                FILTER_MARKET_CAP_ENABLED = True
                                FILTER_VOLUME_24H_ENABLED = True
                                FILTER_CHANGE_24H_ENABLED = True
                                FILTER_HOUR_CANDLE_ENABLED = True
                                STRENGTH_THRESHOLD = 0.46
                                SCALP_MIN_PROFIT = 0.46
                                buying_committee.apply_multiplier(1.0)
                                STOP_LOSS_PARTIAL_1_PERCENT = 0.0144
                                STOP_LOSS_PARTIAL_2_PERCENT = 0.0155
                                STOP_LOSS_FULL_PERCENT = 0.0166
                                CUSTOM_ACTIVE_SYMBOLS_LIMIT = 12
                                CUSTOM_MAX_EXPOSED_PERCENT = 1.0
                                CUSTOM_MAX_DAILY_TRADES = 45
                                CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP = 60
                                PAUSE_NEW_ENTRIES = False
                                COOLDOWN_WIN_HOURS = 8.0
                                COOLDOWN_LOSS_MINUTES = 120.0
                                with _cooldown_lock:
                                    _symbol_cooldown_until.clear()
                                SINGLE_MODEL_FILTER_MODEL = 'rule'
                                SINGLE_MODEL_FILTER_THRESHOLD = 0.11
                                SINGLE_MODEL_FILTER_TIMEFRAME = '5m'
                                SINGLE_MODEL_FILTER_ENABLED = True
                                SECOND_MODEL_FILTER_MODEL = 'cfhm'
                                SECOND_MODEL_FILTER_THRESHOLD = 0.69
                                SECOND_MODEL_FILTER_TIMEFRAME = '5m'
                                SECOND_MODEL_FILTER_ENABLED = True
                                THIRD_MODEL_FILTER_MODEL = 'cfhm'
                                THIRD_MODEL_FILTER_THRESHOLD = 0.55
                                THIRD_MODEL_FILTER_TIMEFRAME = '1h'
                                THIRD_MODEL_FILTER_ENABLED = True
                                FOURTH_MODEL_FILTER_MODEL = 'vwap_obv'
                                FOURTH_MODEL_FILTER_THRESHOLD = 0.44
                                FOURTH_MODEL_FILTER_TIMEFRAME = '15m'
                                FOURTH_MODEL_FILTER_ENABLED = True
                                FIFTH_MODEL_FILTER_MODEL = 'timing'
                                FIFTH_MODEL_FILTER_THRESHOLD = 0.75
                                FIFTH_MODEL_FILTER_TIMEFRAME = '1h'
                                FIFTH_MODEL_FILTER_ENABLED = True
                                SIXTH_MODEL_FILTER_MODEL = 'vwap_obv'
                                SIXTH_MODEL_FILTER_THRESHOLD = 0.47
                                SIXTH_MODEL_FILTER_TIMEFRAME = '1h'
                                SIXTH_MODEL_FILTER_ENABLED = True
                                SEVENTH_MODEL_FILTER_MODEL = 'timing'
                                SEVENTH_MODEL_FILTER_THRESHOLD = 0.25
                                SEVENTH_MODEL_FILTER_TIMEFRAME = '15m'
                                SEVENTH_MODEL_FILTER_ENABLED = True
                                USE_1H_MULTIPLIER = True
                                TRAILING_DISTANCE_PERCENT = 0.04
                                WEIGHT_5M = 40.0
                                WEIGHT_15M = 12.0
                                WEIGHT_1H = 48.0
                                PAUSE_ANALYSIS = False
                                buying_committee.weights = {'rule':0.0, 'emr':0.0, 'cfhm':0.0, 'timing':1.0, 'vwap_obv':0.0}
                                UPPER_THRESHOLD_GLOBAL = 0.85
                                POSITION_SIZE_PERCENT = 0.98
                                _prev_active_symbols_limit = CUSTOM_ACTIVE_SYMBOLS_LIMIT
                                send_telegram("🟢 تم إعادة ضبط جميع الفلاتر والإعدادات الديناميكية إلى القيم الافتراضية الجديدة.")
                                save_state()
                            
                            # ========== أمر رسم منحنى الرصيد ==========
                            elif text == 'منحنى' or text == 'رسم':
                                try:
                                    try:
                                        import matplotlib
                                        matplotlib.use('Agg')
                                        import matplotlib.pyplot as plt
                                    except ImportError:
                                        send_telegram("📦 جاري تثبيت مكتبة matplotlib لإنشاء الرسم... قد يستغرق دقيقة.")
                                        subprocess.check_call([sys.executable, "-m", "pip", "install", "matplotlib"])
                                        import matplotlib
                                        matplotlib.use('Agg')
                                        import matplotlib.pyplot as plt
                                    points = list(bot_stats.equity_curve)[-300:]
                                    if len(points) < 2:
                                        send_telegram("⚠️ لا توجد بيانات كافية لرسم المنحنى (أقل من نقطتين).")
                                        continue
                                    times = []
                                    values = []
                                    for p in points:
                                        try:
                                            t = datetime.fromisoformat(p['time'])
                                            times.append(t)
                                            values.append(p['equity'])
                                        except:
                                            continue
                                    if len(times) < 2:
                                        send_telegram("⚠️ بيانات غير صالحة للرسم.")
                                        continue
                                    plt.figure(figsize=(12, 6))
                                    plt.plot(times, values, marker='.', linestyle='-', linewidth=1.5, markersize=3, color='blue')
                                    plt.title('📈 منحنى الرصيد الكلي (Total Equity) – آخر {} نقطة'.format(len(points)), fontsize=14)
                                    plt.xlabel('الوقت', fontsize=12)
                                    plt.ylabel('الرصيد الكلي (USDT)', fontsize=12)
                                    plt.grid(True, linestyle='--', alpha=0.6)
                                    plt.xticks(rotation=45)
                                    plt.tight_layout()
                                    buf = io.BytesIO()
                                    plt.savefig(buf, format='png', dpi=100)
                                    buf.seek(0)
                                    plt.close()
                                    send_telegram_photo(buf, f'📊 منحنى الرصيد الكلي (آخر {len(points)} نقطة)')
                                except Exception as e:
                                    logger.error(f"خطأ في رسم المنحنى: {e}")
                                    send_telegram(f"⚠️ خطأ أثناء إنشاء الرسم: {str(e)[:100]}")
            time.sleep(1)
        except Exception as e:
            logger.error(f"خطأ في تلغرام: {e}. سأحاول مجدداً بعد 10 ثوانٍ.")
            generate_error_report("فشل_خيط", "تلغرام", str(e))
            time.sleep(10)

# --------------------------- كلاس MarketScanner ---------------------------
class MarketScanner:
    def __init__(self):
        self.last_scan = 0
        self.candidates = []
        self.scores = {}
        self.next_scan_time = 0
    def scan(self):
        global _last_scan_candidates
        tickers = fetch_tickers_with_retry()
        if not tickers:
            send_telegram("⚠️ فشل جلب بيانات الأسواق أثناء المسح.")
            return
        filtered = []
        for sym, data in tickers.items():
            if not sym.endswith('/USDT'): continue
            chg = data.get('percentage', 0.0)
            vol = data.get('quoteVolume', 0.0)
            if FILTER_CHANGE_24H_ENABLED and chg < MIN_24H_CHANGE_PERCENT: continue
            if FILTER_VOLUME_24H_ENABLED and vol < max(MIN_24H_VOLUME_USD, MIN_VOLUME_USD): continue
            if sym not in BASE_SYMBOLS_SET and FILTER_MARKET_CAP_ENABLED:
                cap = get_market_cap_from_coingecko(sym, data)
                if cap < MIN_MARKET_CAP_USD: continue
            current_price = data.get('last', 1)
            hl = 1.0
            score = min(chg/8,6) + min(np.log10(max(vol,1))/5,5) + min((hl-1)*40,3)
            filtered.append((sym, score))
        filtered.sort(key=lambda x:x[1], reverse=True)
        self.candidates = [x[0] for x in filtered[:TOP_CANDIDATES_COUNT]]
        self.scores = {x[0]:round(x[1],1) for x in filtered[:TOP_CANDIDATES_COUNT]}
        new5 = self.candidates[:5]
        if not new5:
            send_telegram("🔍 مسح السوق: لا توجد أي عملات تفي بالشروط حالياً.")
        else:
            if new5 != _last_scan_candidates:
                msg = "<b>🔍 أفضل 5 فرص ساخنة (جديدة)</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in new5)
                send_telegram(msg)
            else:
                next_candidates = self.candidates[5:9]
                if next_candidates:
                    msg = "<b>🔄 فرص بديلة للتحليل (القائمة الأولى ثابتة)</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in next_candidates)
                    send_telegram(msg)
                else:
                    msg = "<b>⚠️ نفس القائمة ولا توجد بدائل</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in new5)
                    send_telegram(msg)
        _last_scan_candidates = new5
        self.last_scan = time.time()
        base_interval = SCAN_INTERVAL_MINUTES * 60
        random_delay = random.randint(0, 30)
        self.next_scan_time = time.time() + base_interval + random_delay
    def should_scan(self):
        return time.time() >= self.next_scan_time

# --------------------------- مراقبة الخيوط وإعادة التشغيل التلقائي ---------------------------
def thread_watchdog():
    essential_threads = {
        'monitor_positions': monitor_positions,
        'background_analyzer': background_analyzer,
        'telegram_polling': telegram_polling,
        'background_scanner': background_scanner,
        'background_balance_updater': background_balance_updater,
        'periodic_sync_pending': periodic_sync_pending,
        'retry_stuck_positions': retry_stuck_positions,
        'balance_recovery_monitor': balance_recovery_monitor,
        'cache_cleanup_thread': cache_cleanup_thread,
        'self_heartbeat': self_heartbeat,
        'health_monitor': health_monitor,
        'periodic_status_report': periodic_status_report,
        'lock_health_monitor': lock_health_monitor,
        'memory_watchdog': memory_watchdog,
        'auto_recovery_monitor': auto_recovery_monitor,
        'cooldown_cleanup': cooldown_cleanup
    }
    for name, target in essential_threads.items():
        t = threading.Thread(target=target, daemon=False, name=name)
        t.start()
        _essential_threads[name] = {'thread': t, 'target': target}
    _essential_threads['websocket_manager'] = {'thread': None, 'target': None}
    thread_restart_counts = defaultdict(int)
    while True:
        try:
            time.sleep(30)
            if not ws_manager.is_alive():
                logger.warning("⚠️ WebSocket Manager توقف. جارٍ إعادة التشغيل...")
                send_telegram("🔄 WebSocket Manager توقف وأعيد تشغيله تلقائياً.")
                ws_manager.restart()
            for name in list(_essential_threads.keys()):
                if name == 'websocket_manager':
                    continue
                info = _essential_threads[name]
                t = info['thread']
                if not t.is_alive():
                    thread_restart_counts[name] += 1
                    restart_count = thread_restart_counts[name]
                    logger.warning(f"⚠️ خيط {name} توقف. جارٍ إعادة التشغيل... (المرة {restart_count})")
                    send_telegram(f"🔄 إعادة تشغيل {name} - المحاولة {restart_count}")
                    new_t = threading.Thread(target=info['target'], daemon=False, name=name)
                    new_t.start()
                    _essential_threads[name]['thread'] = new_t
                    if restart_count > 10:
                        logger.critical(f"❌ {name} يتعطل كثيراً - انتظار 5 دقائق قبل متابعة المحاولات")
                        time.sleep(300)
                        thread_restart_counts[name] = 0
            if _processing_lock.locked():
                lock_age = time.time() - _last_processing_lock_released
                if lock_age > LOCK_TIMEOUT_SECONDS:
                    logger.critical(f"🚨 قفل _processing_lock عالق لمدة {lock_age:.0f} ثانية! تحرير قسري.")
                    send_telegram(f"⚠️ قفل التحليل عالق لمدة {lock_age:.0f} ثانية - تحرير قسري")
                    force_unlock()
        except Exception as e:
            logger.critical(f"⚠️ خطأ في thread_watchdog: {e}")
            generate_error_report("فشل_خيط", "watchdog", str(e), traceback.format_exc())
            time.sleep(10)

# --------------------------- دوال حفظ واستعادة الحالة (معدلة) ---------------------------
def save_state():
    if not _global_state_lock.acquire(timeout=5):
        logger.warning("⚠️ فشل الحصول على _global_state_lock في save_state")
        return
    try:
        if len(bot_stats.equity_curve) > 1000:
            bot_stats.equity_curve = deque(list(bot_stats.equity_curve)[-1000:], maxlen=1000)
        if len(bot_stats.symbol_performance) > 500:
            sorted_items = sorted(bot_stats.symbol_performance.items(), key=lambda x: x[1], reverse=True)
            bot_stats.symbol_performance = dict(sorted_items[:500])
        pos_dict = {}
        for sym, p in open_positions.items():
            tp_levels_serializable = [[t, pct] for t, pct in p.take_profit_levels]
            pos_dict[sym] = {
                'symbol':p.symbol,'side':p.side,'total_size':p.total_size,'remaining_size':p.remaining_size,
                'entry_price':p.entry_price,'highest_price':p.highest_price,'lowest_price':p.lowest_price,
                'stop_loss':p.stop_loss,'symbol_type':p.symbol_type,'take_profit_levels':tp_levels_serializable,
                'trailing_stop':p.trailing_stop,'trailing_activated':p.trailing_activated,'atr':p.atr,
                'open_time':p.open_time.isoformat(),'closed_pnl':p.closed_pnl,
                'pred':p.pred,'confidence':p.confidence,'regime':p.regime,
                'initial_momentum':p.initial_momentum,
                'entry_momentum_time':p.entry_momentum_time.isoformat() if p.entry_momentum_time else None,
                'momentum_decay_threshold':p.momentum_decay_threshold,
                'momentum_check_minutes':p.momentum_check_minutes,
                'max_no_profit_minutes':p.max_no_profit_minutes,
                'last_fail_time':p.last_fail_time.isoformat() if p.last_fail_time else None,
                'retry_count':p.retry_count,
                'crash_monitor_start':p.crash_monitor_start.isoformat() if p.crash_monitor_start else None,
                'lowest_drop':p.lowest_drop,
                '_closing': False,
                'last_target_hit_time': p.last_target_hit_time.isoformat() if p.last_target_hit_time else None,
                'last_target_hit_index': p.last_target_hit_index,
                'sold_at_15': p.sold_at_15,
                'sold_at_20': p.sold_at_20,
                'tp_hit_count': p.tp_hit_count,
                'ai_approved': p.ai_approved,
                'trade_id': p.trade_id,
                'scores_5m': p.scores_5m,
                'scores_15m': p.scores_15m,
                'scores_1h': p.scores_1h,
                'weighted_score_5m': p.weighted_score_5m,
                'weighted_score_15m': p.weighted_score_15m,
                'weighted_score_1h': p.weighted_score_1h,
                'final_score': p.final_score
            }
        model_weights = buying_committee.weights if 'buying_committee' in globals() else {'rule':0.0, 'emr':0.0, 'cfhm':0.0, 'timing':1.0, 'vwap_obv':0.0}
        data = {
            'open_positions':pos_dict,
            '_daily_loss_tracker':_daily_loss_tracker,
            '_daily_trades_count':_daily_trades_count,
            '_daily_trades_date':_daily_trades_date.isoformat() if _daily_trades_date else None,
            '_daily_winning_trades':_daily_winning_trades,
            '_daily_losing_trades':_daily_losing_trades,
            'PAUSED':_is_paused(),
            'daily_loss_cooldown_until':daily_loss_cooldown_until.isoformat() if daily_loss_cooldown_until else None,
            'bot_stats':{
                'total_pnl_usdt':bot_stats.total_pnl_usdt,
                'winning_trades':bot_stats.winning_trades,
                'losing_trades':bot_stats.losing_trades,
                'total_trades':bot_stats.total_trades,
                'daily_pnl':bot_stats.daily_pnl,
                'weekly_pnl':bot_stats.weekly_pnl,
                'weekly_wins':bot_stats.weekly_wins,
                'weekly_losses':bot_stats.weekly_losses,
                'last_balance':bot_stats.last_balance,
                'start_time':bot_stats.start_time.isoformat(),
                'equity_curve':list(bot_stats.equity_curve)[-500:],
                'symbol_performance':bot_stats.symbol_performance,
                'last_week_number':bot_stats.last_week_number
            },
            '_local_pending_symbols':list(_local_pending_symbols),
            '_exchange_pending_symbols':list(_exchange_pending_symbols),
            'filters': {
                'liquidity': FILTER_LIQUIDITY_ENABLED,
                'market_cap': FILTER_MARKET_CAP_ENABLED,
                'volume_24h': FILTER_VOLUME_24H_ENABLED,
                'change_24h': FILTER_CHANGE_24H_ENABLED,
                'hour_candle': FILTER_HOUR_CANDLE_ENABLED,
                'buy_committee_multiplier': CURRENT_BUY_COMMITTEE_MULTIPLIER,
                'strength_threshold': STRENGTH_THRESHOLD,
                'scalp_min_profit': SCALP_MIN_PROFIT,
                'stop_loss_25': STOP_LOSS_PARTIAL_1_PERCENT,
                'stop_loss_33': STOP_LOSS_PARTIAL_2_PERCENT,
                'stop_loss_100': STOP_LOSS_FULL_PERCENT,
                'single_model_filter_model': SINGLE_MODEL_FILTER_MODEL,
                'single_model_filter_threshold': SINGLE_MODEL_FILTER_THRESHOLD,
                'single_model_filter_timeframe': SINGLE_MODEL_FILTER_TIMEFRAME,
                'single_model_filter_enabled': SINGLE_MODEL_FILTER_ENABLED,
                'second_model_filter_model': SECOND_MODEL_FILTER_MODEL,
                'second_model_filter_threshold': SECOND_MODEL_FILTER_THRESHOLD,
                'second_model_filter_timeframe': SECOND_MODEL_FILTER_TIMEFRAME,
                'second_model_filter_enabled': SECOND_MODEL_FILTER_ENABLED,
                'third_model_filter_model': THIRD_MODEL_FILTER_MODEL,
                'third_model_filter_threshold': THIRD_MODEL_FILTER_THRESHOLD,
                'third_model_filter_timeframe': THIRD_MODEL_FILTER_TIMEFRAME,
                'third_model_filter_enabled': THIRD_MODEL_FILTER_ENABLED,
                'fourth_model_filter_model': FOURTH_MODEL_FILTER_MODEL,
                'fourth_model_filter_threshold': FOURTH_MODEL_FILTER_THRESHOLD,
                'fourth_model_filter_timeframe': FOURTH_MODEL_FILTER_TIMEFRAME,
                'fourth_model_filter_enabled': FOURTH_MODEL_FILTER_ENABLED,
                'fifth_model_filter_model': FIFTH_MODEL_FILTER_MODEL,
                'fifth_model_filter_threshold': FIFTH_MODEL_FILTER_THRESHOLD,
                'fifth_model_filter_timeframe': FIFTH_MODEL_FILTER_TIMEFRAME,
                'fifth_model_filter_enabled': FIFTH_MODEL_FILTER_ENABLED,
                'sixth_model_filter_model': SIXTH_MODEL_FILTER_MODEL,
                'sixth_model_filter_threshold': SIXTH_MODEL_FILTER_THRESHOLD,
                'sixth_model_filter_timeframe': SIXTH_MODEL_FILTER_TIMEFRAME,
                'sixth_model_filter_enabled': SIXTH_MODEL_FILTER_ENABLED,
                'seventh_model_filter_model': SEVENTH_MODEL_FILTER_MODEL,
                'seventh_model_filter_threshold': SEVENTH_MODEL_FILTER_THRESHOLD,
                'seventh_model_filter_timeframe': SEVENTH_MODEL_FILTER_TIMEFRAME,
                'seventh_model_filter_enabled': SEVENTH_MODEL_FILTER_ENABLED,
                'trailing_distance_percent': TRAILING_DISTANCE_PERCENT,
                'upper_threshold_global': UPPER_THRESHOLD_GLOBAL,
            },
            'custom_settings': {
                'active_symbols_limit': CUSTOM_ACTIVE_SYMBOLS_LIMIT,
                'max_exposed_percent': CUSTOM_MAX_EXPOSED_PERCENT,
                'max_daily_trades': CUSTOM_MAX_DAILY_TRADES,
                'max_hold_minutes_before_tp': CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP,
                'position_size_percent': POSITION_SIZE_PERCENT
            },
            'pause_new_entries': PAUSE_NEW_ENTRIES,
            'cooldown_settings': {
                'win_hours': COOLDOWN_WIN_HOURS,
                'loss_minutes': COOLDOWN_LOSS_MINUTES,
                'symbol_cooldown': {k: v.isoformat() for k, v in _symbol_cooldown_until.items()}
            },
            'use_1h_multiplier': USE_1H_MULTIPLIER,
            'weights': {
                '5m': WEIGHT_5M,
                '15m': WEIGHT_15M,
                '1h': WEIGHT_1H,
                'models': model_weights
            },
            '_prev_active_symbols_limit': _prev_active_symbols_limit
        }
        tmp_file = STATE_FILE + ".tmp"
        with open(tmp_file, 'w') as f: json.dump(data, f, indent=2)
        if os.path.exists(STATE_FILE):
            try: os.replace(STATE_FILE, STATE_BAK_FILE)
            except: pass
        os.replace(tmp_file, STATE_FILE)
    except Exception as e:
        logger.error(f"فشل حفظ الحالة: {e}")
        generate_error_report("فشل_حالة", "حفظ_حالة", str(e))
    finally: _global_state_lock.release()

def load_state():
    global open_positions, _daily_loss_tracker, _daily_trades_count, _daily_trades_date
    global PAUSED, daily_loss_cooldown_until, bot_stats
    global _daily_winning_trades, _daily_losing_trades, _local_pending_symbols, _exchange_pending_symbols
    global FILTER_LIQUIDITY_ENABLED, FILTER_MARKET_CAP_ENABLED
    global FILTER_VOLUME_24H_ENABLED, FILTER_CHANGE_24H_ENABLED, FILTER_HOUR_CANDLE_ENABLED
    global CURRENT_BUY_COMMITTEE_MULTIPLIER, STRENGTH_THRESHOLD, SCALP_MIN_PROFIT
    global STOP_LOSS_PARTIAL_1_PERCENT, STOP_LOSS_PARTIAL_2_PERCENT, STOP_LOSS_FULL_PERCENT
    global buying_committee
    global CUSTOM_ACTIVE_SYMBOLS_LIMIT, CUSTOM_MAX_EXPOSED_PERCENT
    global CUSTOM_MAX_DAILY_TRADES, CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP
    global PAUSE_NEW_ENTRIES
    global COOLDOWN_WIN_HOURS, COOLDOWN_LOSS_MINUTES, _symbol_cooldown_until
    global USE_1H_MULTIPLIER
    global TRAILING_DISTANCE_PERCENT
    global WEIGHT_5M, WEIGHT_15M, WEIGHT_1H
    global SINGLE_MODEL_FILTER_MODEL, SINGLE_MODEL_FILTER_THRESHOLD, SINGLE_MODEL_FILTER_TIMEFRAME, SINGLE_MODEL_FILTER_ENABLED
    global SECOND_MODEL_FILTER_MODEL, SECOND_MODEL_FILTER_THRESHOLD, SECOND_MODEL_FILTER_TIMEFRAME, SECOND_MODEL_FILTER_ENABLED
    global THIRD_MODEL_FILTER_MODEL, THIRD_MODEL_FILTER_THRESHOLD, THIRD_MODEL_FILTER_TIMEFRAME, THIRD_MODEL_FILTER_ENABLED
    global FOURTH_MODEL_FILTER_MODEL, FOURTH_MODEL_FILTER_THRESHOLD, FOURTH_MODEL_FILTER_TIMEFRAME, FOURTH_MODEL_FILTER_ENABLED
    global FIFTH_MODEL_FILTER_MODEL, FIFTH_MODEL_FILTER_THRESHOLD, FIFTH_MODEL_FILTER_TIMEFRAME, FIFTH_MODEL_FILTER_ENABLED
    global SIXTH_MODEL_FILTER_MODEL, SIXTH_MODEL_FILTER_THRESHOLD, SIXTH_MODEL_FILTER_TIMEFRAME, SIXTH_MODEL_FILTER_ENABLED
    global SEVENTH_MODEL_FILTER_MODEL, SEVENTH_MODEL_FILTER_THRESHOLD, SEVENTH_MODEL_FILTER_TIMEFRAME, SEVENTH_MODEL_FILTER_ENABLED
    global UPPER_THRESHOLD_GLOBAL
    global POSITION_SIZE_PERCENT
    global _prev_active_symbols_limit

    def _load_from_file(filepath):
        if not os.path.exists(filepath): return None
        try:
            with open(filepath, 'r') as f: return json.load(f)
        except Exception as e: logger.warning(f"فشل تحميل {filepath}: {e}"); return None
    data = _load_from_file(STATE_FILE)
    if data is None:
        data = _load_from_file(STATE_BAK_FILE)
        if data: logger.info("✅ تم استعادة الحالة من النسخة الاحتياطية")
        else: logger.info("ℹ️ لا توجد حالة سابقة. البدء من الصفر."); return
    if not _global_state_lock.acquire(timeout=5):
        logger.warning("⚠️ فشل الحصول على _global_state_lock في load_state")
        return
    try:
        for sym, pdata in data.get('open_positions', {}).items():
            tp_levels = [[t, pct] for t, pct in pdata.get('take_profit_levels', [])]
            scores_5m = pdata.get('scores_5m', {})
            scores_15m = pdata.get('scores_15m', {})
            scores_1h = pdata.get('scores_1h', {})
            weighted_score_5m = pdata.get('weighted_score_5m')
            weighted_score_15m = pdata.get('weighted_score_15m')
            weighted_score_1h = pdata.get('weighted_score_1h')
            final_score = pdata.get('final_score')
            pos = Position(pdata['symbol'], pdata['side'], pdata['total_size'], pdata['entry_price'], pdata['atr'],
                           pdata['stop_loss'], None, pdata['symbol_type'], pdata['pred'], pdata['confidence'],
                           pdata['regime'], tp_levels, ai_approved=pdata.get('ai_approved', False),
                           scores_5m=scores_5m, scores_15m=scores_15m, scores_1h=scores_1h,
                           weighted_score_5m=weighted_score_5m, weighted_score_15m=weighted_score_15m,
                           weighted_score_1h=weighted_score_1h, final_score=final_score)
            pos.remaining_size = pdata['remaining_size']
            pos.highest_price = pdata['highest_price']
            pos.lowest_price = pdata['lowest_price']
            pos.trailing_stop = pdata['trailing_stop']
            pos.trailing_activated = pdata['trailing_activated']
            pos.open_time = datetime.fromisoformat(pdata['open_time'])
            pos.closed_pnl = pdata['closed_pnl']
            pos.initial_momentum = pdata['initial_momentum']
            pos.entry_momentum_time = datetime.fromisoformat(pdata['entry_momentum_time']) if pdata.get('entry_momentum_time') else None
            pos.momentum_decay_threshold = pdata['momentum_decay_threshold']
            pos.momentum_check_minutes = pdata['momentum_check_minutes']
            pos.max_no_profit_minutes = pdata['max_no_profit_minutes']
            pos.last_fail_time = datetime.fromisoformat(pdata['last_fail_time']) if pdata.get('last_fail_time') else None
            pos.retry_count = pdata.get('retry_count',0)
            pos.crash_monitor_start = datetime.fromisoformat(pdata['crash_monitor_start']) if pdata.get('crash_monitor_start') else None
            pos.lowest_drop = pdata.get('lowest_drop',0.0)
            pos._closing = False
            pos.last_target_hit_time = datetime.fromisoformat(pdata['last_target_hit_time']) if pdata.get('last_target_hit_time') else None
            pos.last_target_hit_index = pdata.get('last_target_hit_index', -1)
            pos.sold_at_15 = pdata.get('sold_at_15', False)
            pos.sold_at_20 = pdata.get('sold_at_20', False)
            pos.tp_hit_count = pdata.get('tp_hit_count', 0)
            pos.trade_id = pdata.get('trade_id')
            open_positions[sym] = pos
        removed = []
        for sym in list(open_positions.keys()):
            if not validate_restored_position(sym, open_positions[sym]):
                removed.append(sym)
                del open_positions[sym]
                with _cooldown_lock:
                    _symbol_cooldown_until[sym] = datetime.now() + timedelta(minutes=COOLDOWN_LOSS_MINUTES)
        if removed: send_telegram(f"⚠️ تم اكتشاف {len(removed)} مركزاً أُغلقت أثناء التعطل: {', '.join(removed)}"); save_state()
        state_age = time.time() - os.path.getmtime(STATE_FILE)
        if state_age > 1800:
            _local_pending_symbols.clear()
            _exchange_pending_symbols.clear()
            logger.info("تم مسح الرموز المعلقة لأن الحالة المحفوظة أقدم من 30 دقيقة")
        else:
            _local_pending_symbols = set(data.get('_local_pending_symbols', []))
            _exchange_pending_symbols = set(data.get('_exchange_pending_symbols', []))
        _daily_loss_tracker = data.get('_daily_loss_tracker',0.0)
        _daily_trades_count = data.get('_daily_trades_count',0)
        _daily_trades_date = datetime.fromisoformat(data['_daily_trades_date']) if data.get('_daily_trades_date') else None
        _daily_winning_trades = data.get('_daily_winning_trades',0)
        _daily_losing_trades = data.get('_daily_losing_trades',0)
        _set_paused(data.get('PAUSED', False))
        daily_loss_cooldown_until = datetime.fromisoformat(data['daily_loss_cooldown_until']) if data.get('daily_loss_cooldown_until') else None
        stats = data.get('bot_stats',{})
        bot_stats.total_pnl_usdt = stats.get('total_pnl_usdt',0.0)
        bot_stats.winning_trades = stats.get('winning_trades',0)
        bot_stats.losing_trades = stats.get('losing_trades',0)
        bot_stats.total_trades = stats.get('total_trades',0)
        bot_stats.daily_pnl = stats.get('daily_pnl',0.0)
        bot_stats.weekly_pnl = stats.get('weekly_pnl',0.0)
        bot_stats.weekly_wins = stats.get('weekly_wins',0)
        bot_stats.weekly_losses = stats.get('weekly_losses',0)
        bot_stats.last_balance = stats.get('last_balance', PAPER_INITIAL_BALANCE if PAPER_TRADING else 10000.0)
        bot_stats.start_time = datetime.fromisoformat(stats['start_time']) if 'start_time' in stats else datetime.now()
        equity = stats.get('equity_curve', [])
        bot_stats.equity_curve = deque(equity[-1000:], maxlen=1000) if equity else deque(maxlen=1000)
        bot_stats.symbol_performance = stats.get('symbol_performance',{})
        bot_stats.last_week_number = stats.get('last_week_number', datetime.now().isocalendar()[1])
        filters = data.get('filters', {})
        FILTER_LIQUIDITY_ENABLED = filters.get('liquidity', True)
        FILTER_MARKET_CAP_ENABLED = filters.get('market_cap', True)
        FILTER_VOLUME_24H_ENABLED = filters.get('volume_24h', True)
        FILTER_CHANGE_24H_ENABLED = filters.get('change_24h', True)
        FILTER_HOUR_CANDLE_ENABLED = filters.get('hour_candle', True)
        CURRENT_BUY_COMMITTEE_MULTIPLIER = filters.get('buy_committee_multiplier', 1.0)
        STRENGTH_THRESHOLD = filters.get('strength_threshold', 0.46)
        SCALP_MIN_PROFIT = filters.get('scalp_min_profit', 0.46)
        STOP_LOSS_PARTIAL_1_PERCENT = filters.get('stop_loss_25', 0.0144)
        STOP_LOSS_PARTIAL_2_PERCENT = filters.get('stop_loss_33', 0.0155)
        STOP_LOSS_FULL_PERCENT = filters.get('stop_loss_100', 0.0166)
        SINGLE_MODEL_FILTER_MODEL = filters.get('single_model_filter_model', 'rule')
        SINGLE_MODEL_FILTER_THRESHOLD = filters.get('single_model_filter_threshold', 0.11)
        SINGLE_MODEL_FILTER_TIMEFRAME = filters.get('single_model_filter_timeframe', '5m')
        SINGLE_MODEL_FILTER_ENABLED = filters.get('single_model_filter_enabled', True)
        SECOND_MODEL_FILTER_MODEL = filters.get('second_model_filter_model', 'cfhm')
        SECOND_MODEL_FILTER_THRESHOLD = filters.get('second_model_filter_threshold', 0.69)
        SECOND_MODEL_FILTER_TIMEFRAME = filters.get('second_model_filter_timeframe', '5m')
        SECOND_MODEL_FILTER_ENABLED = filters.get('second_model_filter_enabled', True)
        THIRD_MODEL_FILTER_MODEL = filters.get('third_model_filter_model', 'cfhm')
        THIRD_MODEL_FILTER_THRESHOLD = filters.get('third_model_filter_threshold', 0.55)
        THIRD_MODEL_FILTER_TIMEFRAME = filters.get('third_model_filter_timeframe', '1h')
        THIRD_MODEL_FILTER_ENABLED = filters.get('third_model_filter_enabled', True)
        FOURTH_MODEL_FILTER_MODEL = filters.get('fourth_model_filter_model', 'vwap_obv')
        FOURTH_MODEL_FILTER_THRESHOLD = filters.get('fourth_model_filter_threshold', 0.44)
        FOURTH_MODEL_FILTER_TIMEFRAME = filters.get('fourth_model_filter_timeframe', '15m')
        FOURTH_MODEL_FILTER_ENABLED = filters.get('fourth_model_filter_enabled', True)
        FIFTH_MODEL_FILTER_MODEL = filters.get('fifth_model_filter_model', 'timing')
        FIFTH_MODEL_FILTER_THRESHOLD = filters.get('fifth_model_filter_threshold', 0.75)
        FIFTH_MODEL_FILTER_TIMEFRAME = filters.get('fifth_model_filter_timeframe', '1h')
        FIFTH_MODEL_FILTER_ENABLED = filters.get('fifth_model_filter_enabled', True)
        SIXTH_MODEL_FILTER_MODEL = filters.get('sixth_model_filter_model', 'vwap_obv')
        SIXTH_MODEL_FILTER_THRESHOLD = filters.get('sixth_model_filter_threshold', 0.47)
        SIXTH_MODEL_FILTER_TIMEFRAME = filters.get('sixth_model_filter_timeframe', '1h')
        SIXTH_MODEL_FILTER_ENABLED = filters.get('sixth_model_filter_enabled', True)
        SEVENTH_MODEL_FILTER_MODEL = filters.get('seventh_model_filter_model', 'timing')
        SEVENTH_MODEL_FILTER_THRESHOLD = filters.get('seventh_model_filter_threshold', 0.25)
        SEVENTH_MODEL_FILTER_TIMEFRAME = filters.get('seventh_model_filter_timeframe', '15m')
        SEVENTH_MODEL_FILTER_ENABLED = filters.get('seventh_model_filter_enabled', True)
        TRAILING_DISTANCE_PERCENT = filters.get('trailing_distance_percent', 0.04)
        UPPER_THRESHOLD_GLOBAL = filters.get('upper_threshold_global', 0.85)
        PAUSE_NEW_ENTRIES = data.get('pause_new_entries', False)
        cust = data.get('custom_settings', {})
        CUSTOM_ACTIVE_SYMBOLS_LIMIT = cust.get('active_symbols_limit', 12)
        CUSTOM_MAX_EXPOSED_PERCENT = cust.get('max_exposed_percent', 1.0)
        CUSTOM_MAX_DAILY_TRADES = cust.get('max_daily_trades', 45)
        CUSTOM_MAX_HOLD_MINUTES_BEFORE_TP = cust.get('max_hold_minutes_before_tp', 60)
        POSITION_SIZE_PERCENT = cust.get('position_size_percent', 0.98)
        cooldown = data.get('cooldown_settings', {})
        COOLDOWN_WIN_HOURS = cooldown.get('win_hours', 8.0)
        COOLDOWN_LOSS_MINUTES = cooldown.get('loss_minutes', 120.0)
        _symbol_cooldown_until.clear()
        for sym, iso in cooldown.get('symbol_cooldown', {}).items():
            try:
                _symbol_cooldown_until[sym] = datetime.fromisoformat(iso)
            except:
                pass
        USE_1H_MULTIPLIER = data.get('use_1h_multiplier', True)
        weights = data.get('weights', {})
        WEIGHT_5M = weights.get('5m', 40.0)
        WEIGHT_15M = weights.get('15m', 12.0)
        WEIGHT_1H = weights.get('1h', 48.0)
        model_weights = weights.get('models', {'rule':0.0, 'emr':0.0, 'cfhm':0.0, 'timing':1.0, 'vwap_obv':0.0})
        buying_committee.weights = model_weights
        if 'buying_committee' in globals(): buying_committee.apply_multiplier(CURRENT_BUY_COMMITTEE_MULTIPLIER)
        _prev_active_symbols_limit = data.get('_prev_active_symbols_limit', CUSTOM_ACTIVE_SYMBOLS_LIMIT)
        logger.info(f"تم استعادة {len(open_positions)} مركزاً")
    except Exception as e:
        logger.error(f"فشل تحميل الحالة: {e}")
        generate_error_report("فشل_حالة", "حفظ_حالة", f"فشل تحميل الحالة: {e}")
    finally: _global_state_lock.release()

def graceful_shutdown(signum=None, frame=None):
    logger.info("🛑 استلام إشارة إيقاف. جاري حفظ الحالة...")
    ws_manager.stop()
    save_state()
    logger.info("✅ تم حفظ الحالة. إيقاف البوت.")
    sys.exit(0)

# --------------------------- بدء التشغيل الرئيسي ---------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"🚀 سيتم استخدام المنفذ: {port}")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("⚠️ TELEGRAM_TOKEN أو TELEGRAM_CHAT_ID غير معرفين. لن تعمل أوامر تلغرام.")
    else:
        logger.info("✅ تم العثور على مفاتيح تلغرام")
    
    wait_time = random.randint(30, 60)
    logger.info(f"⏳ بدء التشغيل: انتظار {wait_time} ثانية عشوائية لتجنب الحظر...")
    send_telegram(f"⏳ بدء التشغيل: انتظار {wait_time} ثانية...")
    time.sleep(wait_time)
    
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        logger.critical("❌ مفاتيح API مفقودة")
        sys.exit(1)
    
    scanner = MarketScanner()
    buying_committee.weights = {'rule': 0.0, 'emr': 0.0, 'cfhm': 0.0, 'timing': 1.0, 'vwap_obv': 0.0}
    logger.info("✅ تم تعيين أوزان النماذج الافتراضية: Timing=100%")
    load_state()
    
    if not load_markets_with_retry(max_retries=3, initial_delay=30):
        logger.critical("❌ فشل تحميل الأسواق. البوت لن يعمل بشكل صحيح.")
        send_telegram("❌ فشل تحميل الأسواق بعد عدة محاولات. تحقق من الاتصال أو انتظر رفع الحظر.")
        sys.exit(1)
    ws_manager.start()
    logger.info("✅ تم بدء تشغيل WebSocket Manager")
    sync_pending_orders()
    if not PAPER_TRADING and not TEST_MODE and ENABLE_TRADING:
        logger.info("🔄 بدء التشغيل في الوضع الحقيقي - جاري جلب الرصيد الحقيقي...")
        send_telegram("⏳ جاري جلب الرصيد الحقيقي (قد يستغرق حتى دقيقتين)...")
        real_balance = fetch_real_balance_with_retry(timeout_seconds=120, retry_interval=5, silent=False)
        if real_balance is not None:
            with _global_state_lock:
                bot_stats.last_balance = real_balance
            logger.info(f"✅ الرصيد الحقيقي: {real_balance:.2f} USDT")
            send_telegram(f"✅ بدء التشغيل في الوضع الحقيقي. الرصيد: ${real_balance:.2f} USDT")
        else:
            logger.error("❌ فشل جلب الرصيد الحقيقي بعد 120 ثانية. سيتم التحول إلى الوضع الورقي.")
            send_telegram("❌ فشل جلب الرصيد الحقيقي. سيتم بدء البوت في **الوضع الورقي** حفاظاً على الأمان.\nيمكنك التبديل لاحقاً باستخدام الأمر `تداول حقيقي` بعد التحقق من الإعدادات.")
            PAPER_TRADING = True
            TEST_MODE = False
            with _global_state_lock:
                bot_stats.last_balance = PAPER_INITIAL_BALANCE
            save_state()
    mode_str = "محاكاة" if TEST_MODE else ("ورقي" if PAPER_TRADING else ("Testnet" if BINANCE_SANDBOX else "حقيقي"))
    logger.info(f"🚀 بدء البوت v29.0 (نسخة مبسطة) - الوضع: {mode_str}")
    send_telegram(f"🚀 بوت v29.0 (النسخة المبسطة بعد التنظيف) يعمل – الوضع: {mode_str}\n"
                  f"✅ تم حذف: التلاعب، الأداء 30 يوم، الذكاء الاصطناعي، التوصيات، لجنة البيع.\n"
                  f"✅ حجم الصفقة ثابت (قابل للتعديل عبر `حجم الصفقة X%`)\n"
                  f"✅ جميع الميزات الأساسية محفوظة (لجنة الشراء، المراقبة، التنفيذ، الفلاتر).")
    
    threading.Thread(target=thread_watchdog, daemon=True, name="watchdog").start()
    atexit.register(save_state)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    try:
        logger.info("🌟 بدء تشغيل خادم الويب...")
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True, use_reloader=False)
    except Exception as e:
        logger.critical(f"❌ فشل تشغيل خادم Flask: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)