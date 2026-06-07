# ========================================================================
# بوت التداول v27.7 – إصدار مستقر مع إصلاح ذاتي كامل (بدون إعادة تشغيل خارجية)
# التعديلات النهائية: توحيد الوقف المسبق للعادي والميم (تفعيل 1.7%، تنفيذ 2.1%)
# ========================================================================

import subprocess, sys, os, logging, time, warnings, threading, atexit, signal, importlib, re, traceback
import logging.handlers
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque, OrderedDict
from contextlib import contextmanager
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

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
SPOOFING_DETECTION = os.environ.get('SPOOFING_DETECTION', 'true').lower() == 'true'

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
MAX_POSITION_PERCENT_GOOD = float(os.environ.get('MAX_POSITION_PERCENT_GOOD', 0.20))
COOLDOWN_HOURS_GOOD = float(os.environ.get('COOLDOWN_HOURS_GOOD', 0.5))
COOLDOWN_HOURS_BAD = float(os.environ.get('COOLDOWN_HOURS_BAD', 1.0))
MAX_EXPOSED_PERCENT = float(os.environ.get('MAX_EXPOSED_PERCENT', 0.65))
MAX_DAILY_LOSS_PERCENT_OF_EXPOSED = float(os.environ.get('MAX_DAILY_LOSS_PERCENT_OF_EXPOSED', 0.066))
COOLDOWN_HOURS_LOSS_LIMIT = float(os.environ.get('COOLDOWN_HOURS_LOSS_LIMIT', 6))
MIN_VOLUME_USD = float(os.environ.get('MIN_VOLUME_USD', 30000))
STRENGTH_THRESHOLD = float(os.environ.get('STRENGTH_THRESHOLD', 0.25))
SCALP_MIN_PROFIT = float(os.environ.get('SCALP_MIN_PROFIT', 0.20))
MAX_SL_PERCENT_NORMAL = float(os.environ.get('MAX_SL_PERCENT_NORMAL', 0.029))
MAX_SL_PERCENT_MEME = float(os.environ.get('MAX_SL_PERCENT_MEME', 0.066))
SCAN_INTERVAL_MINUTES = int(os.environ.get('SCAN_INTERVAL_MINUTES', 8))
TOP_CANDIDATES_COUNT = int(os.environ.get('TOP_CANDIDATES_COUNT', 30))
ACTIVE_SYMBOLS_LIMIT = int(os.environ.get('ACTIVE_SYMBOLS_LIMIT', 15))
MAX_DAILY_TRADES = int(os.environ.get('MAX_DAILY_TRADES', 30))
BASE_COOLDOWN_HOURS = float(os.environ.get('BASE_COOLDOWN_HOURS', 1.0))
MIN_24H_CHANGE_PERCENT = float(os.environ.get('MIN_24H_CHANGE_PERCENT', 1.0))
MIN_24H_VOLUME_USD = float(os.environ.get('MIN_24H_VOLUME_USD', 100_000))
MIN_MARKET_CAP_USD = float(os.environ.get('MIN_MARKET_CAP_USD', 2_000_000))
NORMAL_MOMENTUM_DECAY_THRESHOLD = float(os.environ.get('NORMAL_MOMENTUM_DECAY_THRESHOLD', 0.25))
NORMAL_MOMENTUM_CHECK_MINUTES = int(os.environ.get('NORMAL_MOMENTUM_CHECK_MINUTES', 15))
NORMAL_MAX_NO_PROFIT_HOLD_MINUTES = int(os.environ.get('NORMAL_MAX_NO_PROFIT_HOLD_MINUTES', 45))
MEME_MOMENTUM_DECAY_THRESHOLD = float(os.environ.get('MEME_MOMENTUM_DECAY_THRESHOLD', 0.15))
MEME_MOMENTUM_CHECK_MINUTES = int(os.environ.get('MEME_MOMENTUM_CHECK_MINUTES', 6))
MEME_MAX_NO_PROFIT_HOLD_MINUTES = int(os.environ.get('MEME_MAX_NO_PROFIT_HOLD_MINUTES', 30))
LIMIT_ORDER_SLIPPAGE = float(os.environ.get('LIMIT_ORDER_SLIPPAGE', 0.001))
LIMIT_ORDER_TIMEOUT_BASE = int(os.environ.get('LIMIT_ORDER_TIMEOUT_BASE', 30))
LIMIT_ORDER_TIMEOUT_MEME = int(os.environ.get('LIMIT_ORDER_TIMEOUT_MEME', 45))
POST_ONLY_ORDERS = os.environ.get('POST_ONLY_ORDERS', 'true').lower() == 'true'
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
SELL_DECISION_THRESHOLD = float(os.environ.get('SELL_DECISION_THRESHOLD', 0.65))
MIN_DEPTH_USD_NORMAL = int(os.environ.get('MIN_DEPTH_USD_NORMAL', 3000))
MIN_DEPTH_USD_MEME = int(os.environ.get('MIN_DEPTH_USD_MEME', 4000))
MAX_SPREAD_NORMAL = float(os.environ.get('MAX_SPREAD_NORMAL', 0.003))
MAX_SPREAD_MEME = float(os.environ.get('MAX_SPREAD_MEME', 0.0144))
MAX_SLIPPAGE_EMERGENCY_NORMAL = float(os.environ.get('MAX_SLIPPAGE_EMERGENCY_NORMAL', 0.02))
MAX_SLIPPAGE_EMERGENCY_MEME = float(os.environ.get('MAX_SLIPPAGE_EMERGENCY_MEME', 0.05))
OVERBOUGHT_RSI_THRESHOLD = float(os.environ.get('OVERBOUGHT_RSI_THRESHOLD', 70.0))
OVERBOUGHT_STOCH_THRESHOLD = float(os.environ.get('OVERBOUGHT_STOCH_THRESHOLD', 80.0))
WHALE_DETECTION_ENABLED = os.environ.get('WHALE_DETECTION_ENABLED', 'true').lower() == 'true'
WHALE_MIN_TRADE_USD_NORMAL = float(os.environ.get('WHALE_MIN_TRADE_USD_NORMAL', 200000.0))
WHALE_MIN_TRADE_USD_MEME = float(os.environ.get('WHALE_MIN_TRADE_USD_MEME', 50000.0))
WHALE_TRADE_IMPACT_THRESHOLD = float(os.environ.get('WHALE_TRADE_IMPACT_THRESHOLD', 0.03))
WHALE_VOLUME_SPIKE_MULTIPLIER = float(os.environ.get('WHALE_VOLUME_SPIKE_MULTIPLIER', 10.0))
SPOOFING_DETECTION_ENABLED = os.environ.get('SPOOFING_DETECTION_ENABLED', 'true').lower() == 'true'
SPOOFING_CANCEL_RATIO_THRESHOLD = float(os.environ.get('SPOOFING_CANCEL_RATIO_THRESHOLD', 0.75))
SPOOFING_ORDER_SIZE_MULTIPLIER_NORMAL = float(os.environ.get('SPOOFING_ORDER_SIZE_MULTIPLIER_NORMAL', 15.0))
SPOOFING_ORDER_SIZE_MULTIPLIER_MEME = float(os.environ.get('SPOOFING_ORDER_SIZE_MULTIPLIER_MEME', 8.0))
SPOOFING_TIME_WINDOW_SECONDS = int(os.environ.get('SPOOFING_TIME_WINDOW_SECONDS', 10))
SPOOFING_MIN_ORDERS = int(os.environ.get('SPOOFING_MIN_ORDERS', 8))
FAKE_LIQUIDITY_ENABLED = os.environ.get('FAKE_LIQUIDITY_ENABLED', 'true').lower() == 'true'
FAKE_WALL_SIZE_MULTIPLIER_NORMAL = float(os.environ.get('FAKE_WALL_SIZE_MULTIPLIER_NORMAL', 15.0))
FAKE_WALL_SIZE_MULTIPLIER_MEME = float(os.environ.get('FAKE_WALL_SIZE_MULTIPLIER_MEME', 8.0))
FAKE_WALL_CANCEL_THRESHOLD = float(os.environ.get('FAKE_WALL_CANCEL_THRESHOLD', 0.85))
MANIPULATION_TIMEOUT = float(os.environ.get('MANIPULATION_TIMEOUT', 5.0))
MANIPULATION_REJECT_THRESHOLD = float(os.environ.get('MANIPULATION_REJECT_THRESHOLD', 0.65))

# --------------------------- عتبات الخسارة الوقائية (قابلة للتعديل عبر تلغرام) ---------------------------
STOP_LOSS_PARTIAL_1_PERCENT = 0.008
STOP_LOSS_PARTIAL_2_PERCENT = 0.012
STOP_LOSS_FULL_PERCENT = 0.017

# --------------------------- متغيرات الوقف المسبق الموحدة للجميع ---------------------------
PREPLACED_SL_TRIGGER_PERCENT = 0.017      # 1.7% تفعيل
PREPLACED_SL_LIMIT_PERCENT = 0.021        # 2.1% تنفيذ مباشر (الحد الأقصى للخسارة)

# --------------------------- المتغيرات العامة ---------------------------
PAUSED = False
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
_last_market_condition = 'normal'
_last_market_condition_time = 0
_market_condition_lock = threading.Lock()
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
last_close_time = {}
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
STALE_DATA_MAX_AGE = int(os.environ.get('STALE_DATA_MAX_AGE', 90))
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
TIMEFRAMES = {'primary': '5m', 'confirm_1': '15m', 'confirm_2': '1h'}
BASE_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']
BASE_SYMBOLS_SET = set(BASE_SYMBOLS)
VOLATILITY_THRESHOLD = 0.03
MEME_PRICE_CHANGE_24H = 5.0
SCALP_TIMEFRAME = '5m'
TAKE_PROFIT_LEVELS_NORMAL = [0.015, 0.018, 0.022, 0.025, 0.029, 0.035, 0.040]
TAKE_PROFIT_LEVELS_MEME = [0.015, 0.022, 0.030, 0.041, 0.051, 0.066, 0.080]
TAKE_PROFIT_PERCENTS_NORMAL = [0.15, 0.15, 0.15, 0.15, 0.20, 0.50, 1.00]
TAKE_PROFIT_PERCENTS_MEME = [0.15, 0.15, 0.15, 0.15, 0.20, 0.50, 1.00]
LOCK_TIMEOUT_SECONDS = 300
_manipulation_cache = {}
_MANIPULATION_CACHE_TTL = 30
FETCH_OHLCV_TIMEOUT = int(os.environ.get('FETCH_OHLCV_TIMEOUT', 200))
FILTER_MANIPULATION_ENABLED = True
FILTER_LIQUIDITY_ENABLED = True
FILTER_MARKET_CAP_ENABLED = True
FILTER_VOLUME_24H_ENABLED = True
FILTER_CHANGE_24H_ENABLED = True
FILTER_HOUR_CANDLE_ENABLED = True
FILTER_4H_CANDLE_ENABLED = True
CURRENT_BUY_COMMITTEE_MULTIPLIER = 1.0
_analysis_failures = 0
_MAX_CONSECUTIVE_FAILURES = 3
_last_force_unlock_time = 0
_MIN_TIME_BETWEEN_FORCE_UNLOCKS = 60

# ------------------- متغيرات auto_recovery الجديدة -------------------
_auto_recovery_failures = 0
_AUTO_RECOVERY_THRESHOLD = 5
_AUTO_RECOVERY_LOCK = threading.Lock()

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

    def add_equity_point(self, pnl):
        self.equity_curve.append({'time': datetime.now().isoformat(), 'pnl': pnl})

bot_stats = BotStats()

# --------------------------- كلاس Position ---------------------------
class Position:
    def __init__(self, symbol, side, size, entry, atr, sl, tp, sym_type, pred, conf, regime, tp_levels=None):
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
        self.preemptive_sl_order_id = None
        self.last_trailing_stop_sent = None
        self.last_target_hit_time = None
        self.last_target_hit_index = -1
        self.sold_at_15 = False
        self.sold_at_20 = False
        self._calc_initial_momentum()
    def _calc_initial_momentum(self):
        try:
            df = fetch_ohlcv_retry(self.symbol, '5m', limit=50)
            if len(df)<15: return
            lookback = max(1, int(self.momentum_check_minutes/5))
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
        config = get_scalp_config(self.symbol_type)
        if profit >= config['trailing_activation']:
            self.trailing_activated = True
            if self.side=='buy':
                new_stop = cur_price * (1 - config['trailing_distance'])
                if self.trailing_stop is None or new_stop > self.trailing_stop:
                    self.trailing_stop = new_stop
            else:
                new_stop = cur_price * (1 + config['trailing_distance'])
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
            else:
                bal = exchange.fetch_balance()
                free = bal.get('USDT', {}).get('free', 0.0)
            with _global_state_lock:
                pos_list = list(open_positions.items())
            pos_val = 0.0
            for sym, p in pos_list:
                ticker = fetch_ticker_with_retry(sym)
                price = ticker['last'] if ticker else p.entry_price
                pos_val += p.remaining_size * price
            total = free + pos_val
            if total > 0 and not PAPER_TRADING:
                with _global_state_lock:
                    bot_stats.last_balance = total
            return total
        except Exception as e:
            logger.warning(f"محاولة {attempt+1} لجلب equity فشلت: {e}")
            time.sleep(1)
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
    'timeout': 60000,
    'options': {
        'defaultType': 'spot',
        'recvWindow': 60000,
        'adjustForTimeDifference': True,
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

logger.info("⏳ انتظار 10 ثوانٍ قبل أول اتصال بـ Binance...")
time.sleep(10)

def get_active_exchange():
    return primary_exchange

# --------------------------- تحميل الأسواق مع صبر شديد ---------------------------
def load_markets_with_retry(max_retries=8, initial_delay=30):
    for attempt in range(max_retries):
        try:
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

# --------------------------- WebSocket Manager مع آلية إعادة تشغيل قوية ---------------------------
class SimpleWebSocketManager:
    def __init__(self):
        self.ticker_cache = {}
        self.lock = threading.Lock()
        self.ws = None
        self.running = False
        self.thread = None
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60

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
        except Exception as e:
            logger.debug(f"خطأ في معالجة رسالة WebSocket: {e}")

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

# --------------------------- Rate Limiter ---------------------------
class RateLimiter:
    def __init__(self, max_calls=50, period=60):
        self.calls = deque()
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            while self.calls and self.calls[0] < now - self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())

rest_rate_limiter = RateLimiter(max_calls=60, period=60)

# --------------------------- دوال التيكرات (تعتمد على WebSocket أولاً) ---------------------------
def fetch_ticker_with_retry(symbol, max_retries=2):
    ticker = ws_manager.get_ticker(symbol)
    if ticker and ticker.get('price', 0) > 0:
        return {'last': ticker['price'], 'percentage': ticker.get('change',0), 'quoteVolume': ticker.get('volume',0)}
    exchange = get_active_exchange()
    for attempt in range(max_retries):
        try:
            rest_rate_limiter.wait_if_needed()
            ticker = exchange.fetch_ticker(symbol)
            if ticker and ticker.get('last') is not None:
                return ticker
        except Exception as e:
            logger.warning(f"جلب السعر {symbol} محاولة {attempt+1}: {e}")
            if "418" in str(e) or "banned" in str(e):
                time.sleep(60)
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
            rest_rate_limiter.wait_if_needed()
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
            if "418" in str(e):
                time.sleep(60)
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
            rest_rate_limiter.wait_if_needed()
            actual_limit = limit
            if timeframe == '4h':
                actual_limit = min(limit, 200)
            bars = exchange.fetch_ohlcv(symbol, timeframe, limit=actual_limit)
            if not bars:
                last_err = "بيانات فارغة"
                attempt += 1
                if attempt < max_retries:
                    time.sleep(2**attempt)
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
                time.sleep(600)
            time.sleep(2**attempt + 2)
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
    df = df.replace([np.inf,-np.inf], np.nan)
    df = df.ffill().bfill().fillna(0)
    return df.astype('float64')

def get_cached_features(symbol, timeframe, limit=500, ttl=30):
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

def get_market_condition():
    tickers = fetch_tickers_with_retry()
    if not tickers:
        return 'normal'
    strong_count = 0
    top_symbols = [sym for sym in tickers if sym.endswith('/USDT')][:100]
    for sym in top_symbols:
        data = tickers.get(sym, {})
        chg = data.get('percentage', 0)
        vol = data.get('quoteVolume', 0)
        if chg > 2.0 and vol > 1_000_000:
            strong_count += 1
    if strong_count <= 5:
        return 'bad'
    elif strong_count <= 15:
        return 'normal'
    else:
        return 'good'

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
        with _market_condition_lock:
            if _last_market_condition == 'good':
                pos_percent = MAX_POSITION_PERCENT_GOOD
            else:
                pos_percent = MAX_POSITION_PERCENT
        position_size_usdt = bot_stats.last_balance * pos_percent
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
<head><meta charset="UTF-8"><title>بوت v27.7</title></head>
<body style="background:#0a0e27;color:#e0e0e0;font-family:sans-serif;padding:20px;">
<h1>🤖 بوت التداول v27.7</h1>
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
            'manipulation_threshold': MANIPULATION_REJECT_THRESHOLD,
            'sell_threshold': SELL_DECISION_THRESHOLD,
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

# --------------------------- دوال تنفيذ الأوامر ---------------------------
def execute_limit_order(symbol, side, size, price_ref, sym_type='normal'):
    timeout = LIMIT_ORDER_TIMEOUT_MEME if sym_type=='meme' else LIMIT_ORDER_TIMEOUT_BASE
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
    for attempt in range(2):
        try:
            rest_rate_limiter.wait_if_needed()
            params = {'postOnly': True} if (POST_ONLY_ORDERS and attempt == 0) else {}
            order = exchange.create_limit_order(symbol, side, size, price_ref, params)
            oid = order['id']
            start = time.time()
            filled = 0.0
            avg_price = 0.0
            while time.time() - start < timeout:
                try:
                    status = exchange.fetch_order(oid, symbol)
                    filled = float(status.get('filled',0))
                    if filled>0:
                        avg_price = float(status.get('average',0)) or status.get('price', price_ref)
                    if status['status'] == 'closed':
                        return True, avg_price, filled, oid
                except Exception:
                    pass
                time.sleep(0.2)
            if filled>0:
                try:
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                return True, avg_price, filled, oid
            else:
                try:
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                if attempt == 0 and POST_ONLY_ORDERS:
                    logger.info(f"⚠️ فشل أمر postOnly لـ {symbol}، إعادة محاولة بدون postOnly")
                    continue
                return False, None, 0, None
        except Exception as e:
            if "418" in str(e) or "banned" in str(e):
                logger.critical(f"🚨 تم حظر IP أثناء أمر {symbol}")
                time.sleep(300)
            if "Post only" in str(e) and attempt == 0:
                continue
            logger.error(f"خطأ في الأمر {symbol}: {e}")
            generate_error_report("فشل_أمر", "أوامر", f"فشل limit order {symbol}: {e}")
            return False, None, 0, None
    return False, None, 0, None

def execute_limit_close(symbol, side, size, price_ref, sym_type='normal', extra_attempt=False):
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
    for attempt in range(2 if extra_attempt else 1):
        try:
            rest_rate_limiter.wait_if_needed()
            order = exchange.create_limit_order(symbol, side, size, price_ref)
            oid = order['id']
            start = time.time()
            filled = 0.0
            avg_price = 0.0
            while time.time() - start < LIMIT_ORDER_TIMEOUT_BASE:
                try:
                    status = exchange.fetch_order(oid, symbol)
                    filled = float(status.get('filled',0))
                    if filled>0:
                        avg_price = float(status.get('average',0)) or status.get('price', price_ref)
                    if status['status'] == 'closed':
                        return True, avg_price, filled, oid
                except Exception:
                    pass
                time.sleep(0.2)
            if filled>0:
                try:
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                return True, avg_price, filled, oid
            else:
                try:
                    exchange.cancel_order(oid, symbol)
                except:
                    pass
                if extra_attempt and attempt == 0:
                    ticker = fetch_ticker_with_retry(symbol)
                    if ticker and ticker.get('last') is not None:
                        price_ref = ticker['last'] * (0.9995 if side=='sell' else 1.0005)
                        price_ref = float(exchange.price_to_precision(symbol, price_ref))
                else:
                    return False, None, 0, None
        except Exception as e:
            logger.error(f"فشل close limit {symbol}: {e}")
            if not extra_attempt or attempt == 1:
                generate_error_report("فشل_أمر", "أوامر", f"فشل close limit {symbol}: {e}")
                return False, None, 0, None
    return False, None, 0, None

# ======================= دالة الوقف المسبق الموحدة (المعدلة) =======================
def place_preemptive_stop_loss(symbol, size, entry_price, sym_type, stop_price_override=None):
    if _should_simulate():
        return None
    exchange = get_active_exchange()
    # استخدام النسب الموحدة للجميع بغض النظر عن sym_type
    if stop_price_override is not None:
        # حالة الوقف المتحرك: استخدم السعر الوارد
        stop_price = stop_price_override
        # سعر التنفيذ: entry_price * (1 - PREPLACED_SL_LIMIT_PERCENT) أو نستخدم نفس النسبة من stop_price؟
        # لكن للحفاظ على الدقة، نستخدم نفس سعر التنفيذ المباشر من الدخول.
        # هنا سنستخدم سعر التنفيذ بناءً على الدخول الأصلي (entry_price) لتحديد الحد الأقصى للخسارة.
        limit_price = entry_price * (1 - PREPLACED_SL_LIMIT_PERCENT)
    else:
        # الوقف الثابت عند فتح الصفقة
        trigger = PREPLACED_SL_TRIGGER_PERCENT      # 1.7%
        limit_pct = PREPLACED_SL_LIMIT_PERCENT      # 2.1%
        stop_price = entry_price * (1 - trigger)
        limit_price = entry_price * (1 - limit_pct)
    # حماية من القيم غير الصالحة
    if limit_price <= 0 or stop_price <= 0:
        logger.error(f"قيم غير صالحة للوقف: stop={stop_price}, limit={limit_price}")
        return None
    try:
        rest_rate_limiter.wait_if_needed()
        market = exchange.market(symbol)
        size = float(exchange.amount_to_precision(symbol, size))
        stop_price = float(exchange.price_to_precision(symbol, stop_price))
        limit_price = float(exchange.price_to_precision(symbol, limit_price))
        params = {'stopPrice': stop_price, 'timeInForce': 'IOC'}
        order = exchange.create_order(symbol, 'stop_loss_limit', 'sell', size, limit_price, params)
        oid = order.get('id', 'unknown')
        logger.info(f"✅ أمر وقف مسبق لـ {symbol} | تفعيل عند {stop_price:.8f} ({(1-stop_price/entry_price)*100:.2f}%) | تنفيذ عند {limit_price:.8f} ({(1-limit_price/entry_price)*100:.2f}%) | ID: {oid}")
        return oid
    except Exception as e:
        logger.error(f"فشل stop_loss_limit لـ {symbol}: {e}")
        # محاولة أمر سوق كحل أخير
        try:
            order = exchange.create_market_sell_order(symbol, size)
            logger.warning(f"⚠️ تم استخدام أمر سوق طارئ لـ {symbol} (فشل الوقف المحدد)")
            return order.get('id')
        except Exception as e2:
            logger.error(f"فشل أمر السوق الطارئ لـ {symbol}: {e2}")
            return None

def cancel_preemptive_stop_loss(symbol, order_id):
    if not order_id or _should_simulate():
        return
    try:
        exchange = get_active_exchange()
        exchange.cancel_order(order_id, symbol)
        logger.info(f"🗑️ تم إلغاء وقف مسبق {symbol} ID: {order_id}")
    except Exception as e:
        logger.warning(f"⚠️ فشل إلغاء وقف مسبق {symbol}: {e}")

def update_preemptive_stop_loss(symbol, pos, new_stop_price):
    if _should_simulate():
        return
    if pos.preemptive_sl_order_id:
        cancel_preemptive_stop_loss(symbol, pos.preemptive_sl_order_id)
        time.sleep(0.3)
    new_oid = place_preemptive_stop_loss(symbol, pos.remaining_size, pos.entry_price, pos.symbol_type, stop_price_override=new_stop_price)
    if new_oid:
        pos.preemptive_sl_order_id = new_oid
        pos.last_trailing_stop_sent = new_stop_price
        logger.debug(f"🔄 تم تحديث الوقف المسبق لـ {symbol} إلى {new_stop_price:.8f}")

def validate_restored_position(symbol, pos):
    if TEST_MODE or PAPER_TRADING or not ENABLE_TRADING:
        return True
    for attempt in range(3):
        try:
            exchange = get_active_exchange()
            rest_rate_limiter.wait_if_needed()
            balance = exchange.fetch_balance()
            coin = symbol.split('/')[0]
            total = balance.get(coin, {}).get('total', 0)
            if total < pos.remaining_size * 0.01:
                logger.warning(f"⚠️ المركز {symbol} لم يعد موجوداً في المنصة. سيتم حذفه.")
                cancel_preemptive_stop_loss(symbol, pos.preemptive_sl_order_id)
                send_telegram(f"ℹ️ تم حذف المركز {symbol} تلقائياً (غير موجود في المحفظة).")
                return False
            return True
        except Exception as e:
            logger.warning(f"خطأ في التحقق من المركز {symbol} (محاولة {attempt+1}/3): {e}")
            time.sleep(1)
    logger.error(f"فشل التحقق من المركز {symbol} بعد 3 محاولات، سيتم الاحتفاظ به افتراضياً")
    return True

def sync_preemptive_stop_orders_once():
    if TEST_MODE or PAPER_TRADING or not ENABLE_TRADING:
        return
    try:
        exchange = get_active_exchange()
        with _global_state_lock:
            symbols_to_check = list(open_positions.keys())
        for sym in symbols_to_check:
            try:
                rest_rate_limiter.wait_if_needed()
                orders = exchange.fetch_open_orders(symbol=sym)
                stop_orders = [o for o in orders if o.get('type') == 'stop_loss_limit']
                order_map = {o['symbol']: o['id'] for o in stop_orders} if stop_orders else {}
                with _global_state_lock:
                    pos = open_positions.get(sym)
                    if not pos:
                        continue
                    current_oid = pos.preemptive_sl_order_id
                    if current_oid and current_oid not in order_map.values():
                        logger.warning(f"⚠️ أمر الوقف المسبق {current_oid} لـ {sym} غير موجود على المنصة. سنرسل أمراً جديداً.")
                        if pos.trailing_activated and pos.trailing_stop is not None:
                            new_stop = pos.trailing_stop
                        else:
                            # استخدام النسبة الموحدة للتنفيذ (2.1%) كحد أقصى
                            new_stop = pos.entry_price * (1 - PREPLACED_SL_TRIGGER_PERCENT)
                        new_oid = place_preemptive_stop_loss(sym, pos.remaining_size, pos.entry_price, pos.symbol_type, stop_price_override=new_stop)
                        if new_oid:
                            pos.preemptive_sl_order_id = new_oid
                            pos.last_trailing_stop_sent = new_stop
                    elif not current_oid and sym not in order_map:
                        if pos.trailing_activated and pos.trailing_stop is not None:
                            new_stop = pos.trailing_stop
                        else:
                            new_stop = pos.entry_price * (1 - PREPLACED_SL_TRIGGER_PERCENT)
                        new_oid = place_preemptive_stop_loss(sym, pos.remaining_size, pos.entry_price, pos.symbol_type, stop_price_override=new_stop)
                        if new_oid:
                            pos.preemptive_sl_order_id = new_oid
                            pos.last_trailing_stop_sent = new_stop
                    elif sym in order_map and current_oid != order_map.get(sym):
                        logger.info(f"🔄 تحديث معرف الوقف المسبق لـ {sym} من {current_oid} إلى {order_map[sym]}")
                        pos.preemptive_sl_order_id = order_map[sym]
            except Exception as e:
                logger.warning(f"فشل مزامنة الوقف المسبق لـ {sym}: {e}")
    except Exception as e:
        logger.warning(f"فشل المزامنة الأولية: {e}")

# --------------------------- النماذج الأساسية ---------------------------
class RuleBasedModel:
    def __init__(self): pass
    def get_signal_percent(self, df):
        if len(df) < 20: return None, None
        close = df['close'].iloc[-1]
        ema9 = df['ema_9'].iloc[-1] if 'ema_9' in df else close
        ema21 = df['ema_21'].iloc[-1] if 'ema_21' in df else close
        rsi = df['rsi'].iloc[-1] if 'rsi' in df else 50
        vol = df['volume'].iloc[-1] if 'volume' in df else 0
        vol_sma = df['volume_sma'].iloc[-1] if 'volume_sma' in df else 1
        high12 = df['high'].iloc[-12:].max() if len(df)>=12 else close
        low12 = df['low'].iloc[-12:].min() if len(df)>=12 else close
        mom = (df['close'].iloc[-1] - df['close'].iloc[-6]) / df['close'].iloc[-6] if len(df)>=6 else 0
        vol_ratio = vol/vol_sma if vol_sma!=0 else 1.0
        stoch_k = df['stoch_rsi_k'].iloc[-1] if 'stoch_rsi_k' in df and not pd.isna(df['stoch_rsi_k'].iloc[-1]) else 50
        buy_score = 0.0
        if ema9>ema21: buy_score += 0.2
        if close>ema9: buy_score += 0.2
        if rsi>55: buy_score += min(0.2, (rsi-55)/150)
        if mom>0.002: buy_score += min(0.2, mom*100)
        if vol_ratio>1.2: buy_score += min(0.1, (vol_ratio-1.2)/10)
        if close/high12>0.97: buy_score += 0.1
        if stoch_k < 30: buy_score += 0.1
        sell_score = 0.0
        if ema9<ema21: sell_score += 0.2
        if close<ema9: sell_score += 0.2
        if rsi<45: sell_score += min(0.2, (45-rsi)/150)
        if mom<-0.002: sell_score += min(0.2, -mom*100)
        if vol_ratio>1.2: sell_score += min(0.1, (vol_ratio-1.2)/10)
        if close/low12<1.03: sell_score += 0.1
        if stoch_k > 70: sell_score += 0.1
        return min(1.0, buy_score), min(1.0, sell_score)

class EMRModel:
    def __init__(self, window=80, coh_th=0.7, amp_th=1.5, phase_history_size=10, cache_ttl=30):
        self.window = window
        self.coh_th = coh_th
        self.amp_th = amp_th
        self.phase_history_size = phase_history_size
        self.symbol_phases = defaultdict(lambda: deque(maxlen=phase_history_size))
        self._phase_timestamps = {}
        self._max_cache_size = 500
        self._last_cleanup = time.time()
        self._result_cache = {}
        self._cache_lock = threading.Lock()
        self.cache_ttl = cache_ttl
    def _get_dominant_cycle(self, prices):
        if len(prices) < self.window: return None,None,None
        y = prices[-self.window:] - np.mean(prices[-self.window:])
        try:
            fft = np.fft.rfft(y)
            freqs = np.fft.rfftfreq(self.window, d=1)
            mag = np.abs(fft)
            if len(mag)>1: mag[0]=0
            idx = np.argmax(mag)
            if mag[idx]==0: return None,None,None
            return freqs[idx], mag[idx]/self.window, np.angle(fft[idx])
        except Exception: return None,None,None
    def _coherence(self, phases_list):
        if len(phases_list) < 2: return 0.5
        arr = np.array(phases_list)
        return np.abs(np.sum(np.exp(1j*arr))) / len(arr)
    def _phase_change(self, phases_list):
        if len(phases_list) < 2: return 0.0
        diffs = [abs(phases_list[i] - phases_list[i-1]) for i in range(1, len(phases_list))]
        return np.mean(diffs)
    def update(self, symbol, df):
        now = time.time()
        with self._cache_lock:
            if symbol in self._result_cache:
                cached_result, cached_time = self._result_cache[symbol]
                if now - cached_time < self.cache_ttl: return cached_result
        if df.empty or len(df) < self.window: result = None
        else:
            prices = df['close'].values
            freq, amp, ph = self._get_dominant_cycle(prices)
            if freq is None: result = None
            else:
                self.symbol_phases[symbol].append(ph)
                self._phase_timestamps[symbol] = now
                if now - self._last_cleanup > 600: self._cleanup_old_entries(now); self._last_cleanup = now
                if len(self.symbol_phases) > self._max_cache_size: self._cleanup_old_entries(now)
                phases = list(self.symbol_phases[symbol])
                coh = self._coherence(phases) if len(phases) >= 2 else 0.5
                phase_change = self._phase_change(phases)
                amp_ratio = amp / (prices[-1]*0.01+1e-6)
                buy_percent = 0.0; sell_percent = 0.0
                if coh >= self.coh_th and amp_ratio > self.amp_th:
                    buy_percent = coh * min(1.0, amp_ratio/self.amp_th)
                elif coh < 0.45 or phase_change > (np.pi / 2.5):
                    sell_percent = (0.5 - coh) * min(1.0, phase_change/(np.pi/2))
                    sell_percent = max(0.1, sell_percent)
                result = {'buy': min(1.0, buy_percent), 'sell': min(1.0, sell_percent)}
        with self._cache_lock:
            self._result_cache[symbol] = (result, now)
            if len(self._result_cache) > 200:
                items = sorted(self._result_cache.items(), key=lambda x: x[1][1])
                for sym, _ in items[:50]: del self._result_cache[sym]
        return result
    def _cleanup_old_entries(self, now, ttl=3600):
        threshold = now - ttl
        for sym in list(self._phase_timestamps.keys()):
            if self._phase_timestamps[sym] < threshold:
                if sym in self.symbol_phases: del self.symbol_phases[sym]
                del self._phase_timestamps[sym]

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

class OverboughtModel:
    def get_sell_vote_percent(self, df, cur_price, pos):
        if len(df) < 20: return 0.0
        rsi = df['rsi'].iloc[-1]
        stoch_k = df['stoch_rsi_k'].iloc[-1] if 'stoch_rsi_k' in df and not pd.isna(df['stoch_rsi_k'].iloc[-1]) else 50
        vol_ratio = df['volume_ratio'].iloc[-1] if 'volume_ratio' in df else 1.0
        if rsi > 75 and stoch_k > 80 and vol_ratio < 0.8: return 0.7
        elif rsi > 70 and stoch_k > 70: return 0.4
        return 0.0

class EngulfingModel:
    def get_sell_vote_percent(self, df, cur_price, pos):
        if len(df) < 3: return 0.0
        try:
            o1, h1, l1, c1 = df['open'].iloc[-2], df['high'].iloc[-2], df['low'].iloc[-2], df['close'].iloc[-2]
            o2, h2, l2, c2 = df['open'].iloc[-1], df['high'].iloc[-1], df['low'].iloc[-1], df['close'].iloc[-1]
            if c1 <= o1: return 0.0
            if c2 >= o2: return 0.0
            if o2 > c1 and c2 < o1:
                if df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.2: return 0.5
                return 0.3
            return 0.0
        except Exception: return 0.0

class LiquidityExitModel:
    def get_sell_vote_percent(self, symbol, cur_price):
        try:
            ob = fetch_orderbook_with_cache(symbol, limit=5)
            if not ob: return 0.0
            bid = ob['bids'][0][0] if ob['bids'] else 0
            ask = ob['asks'][0][0] if ob['asks'] else 0
            if bid <= 0 or ask <= 0: return 0.0
            spread = (ask - bid) / bid
            if spread > 0.003: return 0.2
            return 0.0
        except Exception: return 0.0

class ExtremeATRModel:
    def get_sell_vote_percent(self, df, cur_price, pos):
        if 'atr' not in df.columns or df['atr'].isna().all(): return 0.0
        atr = df['atr'].iloc[-1]
        if atr <= 0: return 0.0
        entry = pos.entry_price
        if pos.side == 'buy': profit = (cur_price - entry) / entry
        else: profit = (entry - cur_price) / entry
        avg_atr_pct = df['atr_percent'].mean() if 'atr_percent' in df.columns else atr / entry
        tp_mult = 2.5 * avg_atr_pct
        sl_mult = 1.5 * avg_atr_pct
        if profit >= tp_mult: return 0.8
        if profit <= -sl_mult: return 0.9
        return 0.0

class MomentumDecayModel:
    def get_sell_vote(self, df, pos, cur_price):
        if pos.initial_momentum is None or pos.entry_momentum_time is None or pos.initial_momentum == 0: return 0.0
        profit = (cur_price - pos.entry_price) / pos.entry_price if pos.side == 'buy' else (pos.entry_price - cur_price) / pos.entry_price
        elapsed_min = (datetime.now() - pos.open_time).total_seconds() / 60
        elapsed_momentum_min = (datetime.now() - pos.entry_momentum_time).total_seconds() / 60
        if elapsed_momentum_min < pos.momentum_check_minutes: return 0.0
        if len(df) < 15: return 0.0
        lookback = max(1, int(pos.momentum_check_minutes / 5))
        if len(df) > lookback:
            price_now = df['close'].iloc[-1]
            price_past = df['close'].iloc[-lookback-1]
            cur_mom = (price_now - price_past) / price_past
        else: return 0.0
        if abs(pos.initial_momentum) < 1e-6: return 0.0
        momentum_ratio = max(0.0, min(1.0, cur_mom / pos.initial_momentum))
        weakness = 1.0 - momentum_ratio
        is_crash = (profit < -0.045 and weakness > 0.95)
        is_very_weak = (weakness > 0.84)
        first_tp = pos.take_profit_levels[0][0] if pos.take_profit_levels else None
        missed_tp_40min = (elapsed_min > 40 and first_tp and cur_price < first_tp)
        if is_crash:
            crash_score = 0.7 + min(0.15, (abs(profit) - 0.045) * 5 + weakness * 0.2)
            return min(0.85, crash_score)
        if is_very_weak:
            if profit < 0:
                loss_factor = min(0.3, abs(profit) * 5)
                return min(0.7, 0.4 + loss_factor + weakness * 0.2)
            elif profit >= 0.011:
                return min(0.7, 0.4 + weakness * 0.3)
        if missed_tp_40min:
            if profit > 0:
                time_factor = min(0.3, (elapsed_min - 40) / 60 * 0.3)
                return 0.5 + time_factor
            else:
                time_factor = min(0.2, (elapsed_min - 40) / 60 * 0.2)
                return 0.2 + time_factor
        return 0.0

# --------------------------- لجنة الشراء ---------------------------
class BuyingCommittee:
    def __init__(self):
        self.models = {'rule': RuleBasedModel(), 'emr': EMRModel(), 'cfhm': CFHMModel()}
        self.DEFAULT_THRESHOLDS = {
            'normal': {
                'bad': {'min_conf':0.50, 'thresh3':0.58, 'thresh2':0.53, 'thresh1':0.50,
                        'weak_penalty_15m':0.15, 'strong_bonus_15m':0.10,
                        'weak_penalty_1h':0.15, 'strong_bonus_1h':0.10},
                'normal': {'min_conf':0.45, 'thresh3':0.53, 'thresh2':0.48, 'thresh1':0.45,
                           'weak_penalty_15m':0.12, 'strong_bonus_15m':0.12,
                           'weak_penalty_1h':0.12, 'strong_bonus_1h':0.12},
                'good': {'min_conf':0.42, 'thresh3':0.50, 'thresh2':0.45, 'thresh1':0.42,
                         'weak_penalty_15m':0.10, 'strong_bonus_15m':0.15,
                         'weak_penalty_1h':0.10, 'strong_bonus_1h':0.15}
            },
            'meme': {
                'bad': {'min_conf':0.46, 'thresh3':0.53, 'thresh2':0.48, 'thresh1':0.46,
                        'weak_penalty_15m':0.12, 'strong_bonus_15m':0.08,
                        'weak_penalty_1h':0.12, 'strong_bonus_1h':0.08},
                'normal': {'min_conf':0.42, 'thresh3':0.48, 'thresh2':0.43, 'thresh1':0.42,
                           'weak_penalty_15m':0.10, 'strong_bonus_15m':0.10,
                           'weak_penalty_1h':0.10, 'strong_bonus_1h':0.10},
                'good': {'min_conf':0.40, 'thresh3':0.45, 'thresh2':0.40, 'thresh1':0.40,
                         'weak_penalty_15m':0.08, 'strong_bonus_15m':0.12,
                         'weak_penalty_1h':0.08, 'strong_bonus_1h':0.12}
            }
        }
        self.original_thresholds = None
        self.apply_multiplier(1.0)
    def apply_multiplier(self, percent_multiplier):
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
        save_state()
        logger.info(f"تم تغيير عتبات لجنة الشراء بمضاعف {percent_multiplier:.2f}")
    def get_multiplier(self): return CURRENT_BUY_COMMITTEE_MULTIPLIER
    def decide(self, df_primary, symbol, sym_type='normal', market_condition='normal'):
        buy_pct_rule, _ = self.models['rule'].get_signal_percent(df_primary)
        buy_pct_cfhm, _ = self.models['cfhm'].calculate_scores(df_primary)
        try:
            emr_res = self.models['emr'].update(symbol, df_primary)
            buy_pct_emr = emr_res['buy'] if emr_res else None
        except Exception: buy_pct_emr = None
        raw_values = [buy_pct_rule, buy_pct_emr, buy_pct_cfhm]
        valid = [v for v in raw_values if v is not None]
        if not valid: return 'neutral', 0.0, 0.0
        df_confirm_1 = get_cached_features(symbol, TIMEFRAMES['confirm_1'], limit=500, ttl=45)
        df_confirm_2 = get_cached_features(symbol, TIMEFRAMES['confirm_2'], limit=500, ttl=45)
        cfg = self.thresholds.get(sym_type, {}).get(market_condition, self.thresholds['normal']['normal'])
        multiplier = 1.0
        if not df_confirm_1.empty and len(df_confirm_1) >= 20:
            confirm_score_15m, _ = self.models['rule'].get_signal_percent(df_confirm_1)
            if confirm_score_15m is not None:
                if confirm_score_15m < 0.4: multiplier -= cfg['weak_penalty_15m']
                elif confirm_score_15m > 0.6: multiplier += cfg['strong_bonus_15m']
        if not df_confirm_2.empty and len(df_confirm_2) >= 20:
            confirm_score_1h, _ = self.models['rule'].get_signal_percent(df_confirm_2)
            if confirm_score_1h is not None:
                if confirm_score_1h < 0.35: multiplier -= cfg['weak_penalty_1h']
                elif confirm_score_1h > 0.55: multiplier += cfg['strong_bonus_1h']
        adjusted = [max(0.0, min(1.0, v * multiplier)) for v in valid]
        avg = sum(adjusted) / len(adjusted)
        thresh_key = f'thresh{len(adjusted)}'
        thresh = cfg.get(thresh_key, cfg['thresh1'])
        if avg > thresh and avg >= cfg['min_conf']: return 'buy', avg, avg
        else: return 'neutral', avg, avg

# --------------------------- لجنة البيع ---------------------------
class SellingCommittee:
    def __init__(self):
        self.models = {
            'rule': RuleBasedModel(), 'emr': EMRModel(), 'cfhm': CFHMModel(),
            'overbought': OverboughtModel(), 'engulfing': EngulfingModel(),
            'liquidity_exit': LiquidityExitModel(), 'atr_ext': ExtremeATRModel(),
            'momentum_decay': MomentumDecayModel()
        }
    def decide_sell(self, df_primary, symbol, pos, cur_price, market_condition='normal'):
        if pos.side == 'buy': profit = (cur_price - pos.entry_price) / pos.entry_price
        else: profit = (pos.entry_price - cur_price) / pos.entry_price
        _, sell_pct_rule = self.models['rule'].get_signal_percent(df_primary)
        _, sell_pct_cfhm = self.models['cfhm'].calculate_scores(df_primary)
        try:
            emr_res = self.models['emr'].update(symbol, df_primary)
            sell_pct_emr = emr_res['sell'] if emr_res else None
        except Exception: sell_pct_emr = None
        momentum_vote = self.models['momentum_decay'].get_sell_vote(df_primary, pos, cur_price)
        df_confirm_1 = get_cached_features(symbol, TIMEFRAMES['confirm_1'], limit=500, ttl=45)
        df_confirm_2 = get_cached_features(symbol, TIMEFRAMES['confirm_2'], limit=500, ttl=45)
        if market_condition == 'bad': weak_penalty_15m, strong_bonus_15m = 0.15, 0.05; weak_penalty_1h, strong_bonus_1h = 0.15, 0.05
        elif market_condition == 'good': weak_penalty_15m, strong_bonus_15m = 0.05, 0.15; weak_penalty_1h, strong_bonus_1h = 0.05, 0.15
        else: weak_penalty_15m, strong_bonus_15m = 0.10, 0.10; weak_penalty_1h, strong_bonus_1h = 0.10, 0.10
        multiplier = 1.0
        if not df_confirm_1.empty and len(df_confirm_1) >= 20:
            _, sell_15m = self.models['rule'].get_signal_percent(df_confirm_1)
            if sell_15m is not None:
                if sell_15m < 0.3: multiplier -= weak_penalty_15m
                elif sell_15m > 0.5: multiplier += strong_bonus_15m
        if not df_confirm_2.empty and len(df_confirm_2) >= 20:
            _, sell_1h = self.models['rule'].get_signal_percent(df_confirm_2)
            if sell_1h is not None:
                if sell_1h < 0.25: multiplier -= weak_penalty_1h
                elif sell_1h > 0.45: multiplier += strong_bonus_1h
        raw_base = [sell_pct_rule, sell_pct_emr, sell_pct_cfhm]
        base_valid = [max(0.0, min(1.0, v * multiplier)) for v in raw_base if v is not None]
        other_votes = [
            self.models['overbought'].get_sell_vote_percent(df_primary, cur_price, pos),
            self.models['engulfing'].get_sell_vote_percent(df_primary, cur_price, pos),
            self.models['liquidity_exit'].get_sell_vote_percent(symbol, cur_price),
            self.models['atr_ext'].get_sell_vote_percent(df_primary, cur_price, pos),
            momentum_vote
        ]
        all_valid = base_valid + other_votes
        valid = [v for v in all_valid if v is not None]
        if not valid: return False, 0.0, {}
        avg = sum(valid) / len(valid)
        if avg >= SELL_DECISION_THRESHOLD: 
            return True, avg, {'percentages': all_valid, 'average': avg}
        else: 
            return False, avg, {'percentages': all_valid, 'average': avg}

# --------------------------- كشف التلاعب ---------------------------
_orderbook_cache = {}
_orderbook_cache_lock = threading.Lock()
_ORDERBOOK_CACHE_MAX = 500
def fetch_orderbook_with_cache(symbol, limit=LIQUIDITY_CHECK_DEPTH, ttl=2):
    now = time.time()
    with _orderbook_cache_lock:
        key = f"{symbol}_{limit}"
        if key in _orderbook_cache:
            entry = _orderbook_cache[key]
            if now - entry['timestamp'] < ttl: return entry['data']
    try:
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

_trades_cache = {}
_trades_cache_lock = threading.Lock()
_TRADES_CACHE_TTL = 10
def fetch_recent_trades(symbol, limit=50, ttl=10):
    now = time.time()
    cache_key = f"{symbol}_{limit}"
    with _trades_cache_lock:
        if cache_key in _trades_cache:
            entry = _trades_cache[cache_key]
            if now - entry['timestamp'] < ttl: return entry['data']
    try:
        exchange = get_active_exchange()
        rest_rate_limiter.wait_if_needed()
        trades = exchange.fetch_trades(symbol, limit=limit)
        if trades:
            with _trades_cache_lock:
                _trades_cache[cache_key] = {'data': trades, 'timestamp': now}
                if len(_trades_cache) > 200:
                    oldest = min(_trades_cache.items(), key=lambda x: x[1]['timestamp'])[0]
                    del _trades_cache[oldest]
            return trades
    except Exception: pass
    return []

class OrderBookManipulationDetector:
    def __init__(self):
        self.order_snapshots = {}
        self.spoofing_history = defaultdict(lambda: deque(maxlen=100))
        self.wall_history = defaultdict(lambda: deque(maxlen=100))
        self.last_cleanup = time.time()
    def _cleanup_old_entries(self, now, ttl=300):
        if now - self.last_cleanup < 60: return
        self.last_cleanup = now
        for sym in list(self.order_snapshots.keys()):
            if now - self.order_snapshots[sym].get('timestamp', 0) > ttl: del self.order_snapshots[sym]
    def detect_spoofing(self, symbol, current_ob, sym_type='normal', window_seconds=10, min_orders=5):
        if current_ob is None or not isinstance(current_ob, dict) or 'bids' not in current_ob or 'asks' not in current_ob:
            return False, 0.0
        if not SPOOFING_DETECTION_ENABLED: return False, 0.0
        now = time.time()
        self._cleanup_old_entries(now)
        if symbol not in self.order_snapshots:
            self.order_snapshots[symbol] = {'timestamp': now, 'bids': current_ob.get('bids', [])[:20], 'asks': current_ob.get('asks', [])[:20]}
            return False, 0.0
        prev = self.order_snapshots[symbol]
        prev_bids_set = {(b[0], b[1]) for b in prev.get('bids', [])}
        prev_asks_set = {(a[0], a[1]) for a in prev.get('asks', [])}
        curr_bids_set = {(b[0], b[1]) for b in current_ob.get('bids', [])[:20]}
        curr_asks_set = {(a[0], a[1]) for a in current_ob.get('asks', [])[:20]}
        avg_depth_bid = np.mean([b[0]*b[1] for b in prev.get('bids', [])[:5]]) if prev.get('bids') else 0
        avg_depth_ask = np.mean([a[0]*a[1] for a in prev.get('asks', [])[:5]]) if prev.get('asks') else 0
        avg_depth = (avg_depth_bid + avg_depth_ask) / 2 if avg_depth_bid + avg_depth_ask > 0 else 1
        size_multiplier = SPOOFING_ORDER_SIZE_MULTIPLIER_MEME if sym_type == 'meme' else SPOOFING_ORDER_SIZE_MULTIPLIER_NORMAL
        large_order_threshold = avg_depth * size_multiplier
        large_orders_cancelled = 0
        total_large_orders = 0
        for a in prev.get('asks', []):
            if a[0]*a[1] > large_order_threshold:
                total_large_orders += 1
                if a in prev_asks_set and a not in curr_asks_set: large_orders_cancelled += 1
        for b in prev.get('bids', []):
            if b[0]*b[1] > large_order_threshold:
                total_large_orders += 1
                if b in prev_bids_set and b not in curr_bids_set: large_orders_cancelled += 1
        cancel_ratio = large_orders_cancelled / total_large_orders if total_large_orders > 0 else 0
        self.spoofing_history[symbol].append({'time': now, 'cancel_ratio': cancel_ratio, 'large_orders': total_large_orders, 'cancelled': large_orders_cancelled})
        self.order_snapshots[symbol]['bids'] = current_ob.get('bids', [])[:20]
        self.order_snapshots[symbol]['asks'] = current_ob.get('asks', [])[:20]
        self.order_snapshots[symbol]['timestamp'] = now
        recent = [h for h in self.spoofing_history[symbol] if now - h['time'] <= window_seconds]
        if len(recent) < min_orders: return False, 0.0
        avg_cancel_ratio = np.mean([h['cancel_ratio'] for h in recent])
        if avg_cancel_ratio > SPOOFING_CANCEL_RATIO_THRESHOLD:
            logger.warning(f"🐍 [SPOOFING] {symbol}: نسبة إلغاء {avg_cancel_ratio:.2%} > {SPOOFING_CANCEL_RATIO_THRESHOLD:.2%}")
            generate_error_report("تلاعب_دفتر", "spoofing", f"{symbol}: نسبة إلغاء {avg_cancel_ratio:.2%}")
            return True, avg_cancel_ratio
        return False, avg_cancel_ratio
    def detect_fake_liquidity(self, symbol, current_ob, sym_type='normal'):
        if current_ob is None or not isinstance(current_ob, dict) or 'bids' not in current_ob or 'asks' not in current_ob: return False, 0.0
        if not FAKE_LIQUIDITY_ENABLED: return False, 0.0
        now = time.time()
        if symbol not in self.order_snapshots: return False, 0.0
        prev = self.order_snapshots[symbol]
        avg_bid_size = np.mean([b[0]*b[1] for b in prev.get('bids', [])[:5]]) if prev.get('bids') else 0
        avg_ask_size = np.mean([a[0]*a[1] for a in prev.get('asks', [])[:5]]) if prev.get('asks') else 0
        avg_size = (avg_bid_size + avg_ask_size) / 2 if avg_bid_size + avg_ask_size > 0 else 1
        wall_multiplier = FAKE_WALL_SIZE_MULTIPLIER_MEME if sym_type == 'meme' else FAKE_WALL_SIZE_MULTIPLIER_NORMAL
        wall_threshold = avg_size * wall_multiplier
        def is_same_price_level(price1, price2, tolerance=0.005):
            return abs(price1 - price2) / max(price1, price2) < tolerance
        prev_large_bids = [(b[0], b[1]) for b in prev.get('bids', [])[:10] if b[0]*b[1] > wall_threshold]
        prev_large_asks = [(a[0], a[1]) for a in prev.get('asks', [])[:10] if a[0]*a[1] > wall_threshold]
        curr_large_bids = [(b[0], b[1]) for b in current_ob.get('bids', [])[:10] if b[0]*b[1] > wall_threshold]
        curr_large_asks = [(a[0], a[1]) for a in current_ob.get('asks', [])[:10] if a[0]*a[1] > wall_threshold]
        vanished_bids = sum(1 for price, size in prev_large_bids if not any(is_same_price_level(price, cp, 0.005) for cp, cs in curr_large_bids))
        vanished_asks = sum(1 for price, size in prev_large_asks if not any(is_same_price_level(price, cp, 0.005) for cp, cs in curr_large_asks))
        total_walls = len(prev_large_bids) + len(prev_large_asks)
        vanished = vanished_bids + vanished_asks
        vanish_ratio = vanished / total_walls if total_walls > 0 else 0
        self.wall_history[symbol].append({'time': now, 'vanish_ratio': vanish_ratio, 'total_walls': total_walls})
        recent = [h for h in self.wall_history[symbol] if now - h['time'] <= SPOOFING_TIME_WINDOW_SECONDS]
        if len(recent) >= 3:
            avg_vanish = np.mean([h['vanish_ratio'] for h in recent])
            if avg_vanish > FAKE_WALL_CANCEL_THRESHOLD and len(curr_large_bids) == 0 and len(curr_large_asks) == 0:
                logger.warning(f"🎭 [FAKE LIQUIDITY] {symbol}: {avg_vanish:.2%} من الجدران اختفت فجأة")
                return True, avg_vanish
        return False, vanish_ratio
    def detect_imbalance(self, symbol, current_ob, sym_type='normal'):
        if current_ob is None or not isinstance(current_ob, dict) or 'bids' not in current_ob or 'asks' not in current_ob: return False, 0.0
        bids = current_ob.get('bids', [])
        asks = current_ob.get('asks', [])
        if not bids or not asks: return False, 0.0
        bid_volume = sum(b[0]*b[1] for b in bids[:10])
        ask_volume = sum(a[0]*a[1] for a in asks[:10])
        if ask_volume == 0: return False, 1.0
        imbalance = bid_volume / ask_volume
        high_thresh = 4.0 if sym_type == 'meme' else 3.0
        low_thresh = 0.25 if sym_type == 'meme' else 0.33
        if imbalance > high_thresh or imbalance < low_thresh:
            logger.info(f"⚖️ [IMBALANCE] {symbol}: نسبة {imbalance:.2f} (غير طبيعي)")
            return True, imbalance
        return False, imbalance

class WhaleManipulationDetector:
    def __init__(self):
        self.trade_history = defaultdict(lambda: deque(maxlen=500))
        self.whale_trades = defaultdict(lambda: deque(maxlen=100))
        self.last_price = {}
        self.last_price_time = {}
        self.volume_history = defaultdict(lambda: deque(maxlen=50))
    def detect_whale_trade(self, symbol, sym_type='normal', current_ob=None):
        if not WHALE_DETECTION_ENABLED: return False, 0.0, "no_whale"
        now = time.time()
        trades = fetch_recent_trades(symbol, limit=50)
        if not trades: return False, 0.0, "no_trades"
        min_whale_usd = WHALE_MIN_TRADE_USD_MEME if sym_type == 'meme' else WHALE_MIN_TRADE_USD_NORMAL
        recent_whale_trades = []
        for trade in trades:
            amount_usd = trade.get('cost', trade.get('amount', 0) * trade.get('price', 0))
            if amount_usd >= min_whale_usd:
                recent_whale_trades.append({'time': trade['timestamp'] / 1000 if isinstance(trade['timestamp'], (int, float)) else now, 'amount_usd': amount_usd, 'price': trade['price'], 'side': trade.get('side', 'unknown')})
        if recent_whale_trades:
            self.whale_trades[symbol].extend(recent_whale_trades)
            time_window = 300
            recent_whales_in_window = [t for t in self.whale_trades[symbol] if now - t['time'] <= time_window]
            whale_score = min(0.9, len(recent_whales_in_window) * 0.3)
            if whale_score > 0.3: logger.info(f"🐋 [WHALE DETECTION] {symbol}: {len(recent_whales_in_window)} صفقة حوت في 5 دقائق، درجة {whale_score:.2f}")
            return False, whale_score, "whale_info"
        if current_ob and recent_whale_trades:
            last_whale = recent_whale_trades[-1]
            best_bid = current_ob['bids'][0][0] if current_ob.get('bids') else last_whale['price']
            best_ask = current_ob['asks'][0][0] if current_ob.get('asks') else last_whale['price']
            mid_price = (best_bid + best_ask) / 2
            price_impact = abs(last_whale['price'] - mid_price) / mid_price
            if price_impact > WHALE_TRADE_IMPACT_THRESHOLD:
                impact_score = min(1.0, price_impact / WHALE_TRADE_IMPACT_THRESHOLD)
                logger.info(f"🐋 [WHALE IMPACT] {symbol}: تأثير سعري {price_impact:.2%} من صفقة بقيمة ${last_whale['amount_usd']:,.0f} (درجة {impact_score:.2f})")
                return False, impact_score, "whale_impact"
        return False, 0.0, "no_whale"
    def detect_volume_spike(self, symbol, sym_type='normal'):
        try:
            df = fetch_ohlcv_retry(symbol, '1m', limit=70)
            if df.empty or len(df) < 20: return False, 0.0
            recent_volume = df['volume'].iloc[-5:].sum()
            avg_volume_per_minute = df['volume'].iloc[-60:].mean() if len(df) >= 60 else df['volume'].mean()
            if avg_volume_per_minute <= 0: return False, 0.0
            expected_volume = avg_volume_per_minute * 5
            if expected_volume <= 0: return False, 0.0
            spike_ratio = recent_volume / expected_volume
            threshold = WHALE_VOLUME_SPIKE_MULTIPLIER * (0.7 if sym_type == 'meme' else 1.0)
            spike_score = min(1.0, spike_ratio / threshold)
            if spike_ratio > threshold:
                logger.warning(f"📊 [VOLUME SPIKE] {symbol}: حجم {recent_volume:,.0f} (متوسط {expected_volume/5:,.0f}/د) نسبة {spike_ratio:.1f}x")
                return True, spike_score
            return False, spike_score
        except Exception: return False, 0.0

order_book_detector = OrderBookManipulationDetector()
whale_detector = WhaleManipulationDetector()
def check_manipulation(symbol, sym_type='normal'):
    now = time.time()
    cache_key = f"{symbol}_{sym_type}"
    with _cache_lock:
        if cache_key in _manipulation_cache and now - _manipulation_cache[cache_key]['timestamp'] < _MANIPULATION_CACHE_TTL:
            return _manipulation_cache[cache_key]['result']
    try:
        ob = fetch_orderbook_with_cache(symbol, limit=50)
        if not ob: result = (False, 0.0, "no_orderbook")
        else:
            is_spoofing, spoofing_score = order_book_detector.detect_spoofing(symbol, ob, sym_type)
            if is_spoofing: result = (True, spoofing_score, "spoofing")
            else:
                is_fake_liq, fake_score = order_book_detector.detect_fake_liquidity(symbol, ob, sym_type)
                if is_fake_liq: result = (True, fake_score, "fake_liquidity")
                else:
                    is_imbalanced, imbalance_score = order_book_detector.detect_imbalance(symbol, ob, sym_type)
                    imbalance_contribution = min(1.0, (abs(imbalance_score - 1) / 2) * 0.1)
                    is_whale, whale_score, whale_type = whale_detector.detect_whale_trade(symbol, sym_type, current_ob=ob)
                    is_volume_spike, volume_score = whale_detector.detect_volume_spike(symbol, sym_type)
                    suspicion_score = (spoofing_score + fake_score + whale_score + volume_score + imbalance_contribution) / 5.0
                    if suspicion_score >= MANIPULATION_REJECT_THRESHOLD:
                        result = (True, suspicion_score, f"suspicious (threshold {MANIPULATION_REJECT_THRESHOLD:.2%})")
                    else:
                        result = (False, suspicion_score, "clean")
        with _cache_lock:
            _manipulation_cache[cache_key] = {'result': result, 'timestamp': now}
            if len(_manipulation_cache) > 500:
                for k in list(_manipulation_cache.keys())[:100]: del _manipulation_cache[k]
        return result
    except Exception as e:
        logger.debug(f"خطأ في كشف التلاعب لـ {symbol}: {e}")
        return (False, 0.0, f"error: {e}")

def trades_cache_cleanup():
    while True:
        time.sleep(300)
        now = time.time()
        with _trades_cache_lock:
            keys_to_delete = [k for k, v in _trades_cache.items() if now - v['timestamp'] > 60]
            for k in keys_to_delete: del _trades_cache[k]
        logger.debug("🧹 تنظيف كاش الصفقات")

# --------------------------- دوال التداول ---------------------------
def log_trade(symbol, side, entry, exit, size, pnl, reason, pred, conf, regime, sym_type):
    trade_logger.info(f"{symbol},{side},{entry},{exit},{size},{pnl},{reason},{pred},{conf},{regime},{sym_type}")
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
    bot_stats.add_equity_point(bot_stats.total_pnl_usdt)
    save_state()

def close_partial(symbol, percent, price, reason, pnl_usdt=None):
    global last_close_time, _daily_loss_tracker, _daily_trades_count, _daily_winning_trades, _daily_losing_trades
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    with _global_state_lock:
        pos = open_positions.get(symbol)
        if not pos: return
        if not validate_restored_position(symbol, pos):
            cancel_preemptive_stop_loss(symbol, pos.preemptive_sl_order_id)
            del open_positions[symbol]
            last_close_time[symbol] = datetime.now()
            _local_pending_symbols.discard(symbol)
            _exchange_pending_symbols.discard(symbol)
            save_state()
            send_telegram(f"ℹ️ تم حذف المركز {symbol} تلقائياً (غير موجود في المحفظة)")
            return
        if pos._closing: return
        pos._closing = True
    try:
        close_size = pos.remaining_size * percent
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
                    ticker = fetch_ticker_with_retry(symbol)
                    curr = ticker['last'] if ticker else price
                    success, fill_price, filled_size, _ = execute_limit_close(symbol, close_side, close_size, curr, pos.symbol_type, extra_attempt=(attempt==MAX_RETRIES_CLOSE-1))
                    if success: break
                    time.sleep(0.8*(attempt+1))
                if not success:
                    try:
                        ticker = fetch_ticker_with_retry(symbol)
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
                log_trade(symbol, pos.side, pos.entry_price, fill_price, filled_size, pnl_net, reason, pos.pred, pos.confidence, pos.regime, pos.symbol_type)
                position_value = pos.entry_price * filled_size
                pnl_percent = (pnl_net / position_value) * 100 if position_value != 0 else 0.0
                direction = "🔴" if pnl_net<0 else "🟢"
                sell_msg = (f"{direction} <b>بيع جزئي</b> {symbol}\nالنسبة: {percent*100:.0f}% | الحجم: {filled_size:.6f}\nالسعر: {fill_price:.8f} | الربح الصافي: {pnl_net:+.4f} USDT ({pnl_percent:+.2f}%)\nالسبب: {reason}")
                send_telegram(sell_msg)
                if pos.remaining_size <= 1e-8:
                    cancel_preemptive_stop_loss(symbol, pos.preemptive_sl_order_id)
                    pos.preemptive_sl_order_id = None
                    if pnl_net>0:
                        bot_stats.winning_trades += 1
                        _daily_winning_trades += 1
                    else:
                        bot_stats.losing_trades += 1
                        _daily_losing_trades += 1
                    bot_stats.total_trades += 1
                    _daily_trades_count += 1
                    del open_positions[symbol]
                    last_close_time[symbol] = datetime.now()
                    _local_pending_symbols.discard(symbol)
                    _exchange_pending_symbols.discard(symbol)
                    total_eq = get_total_equity() if not TEST_MODE and not PAPER_TRADING and ENABLE_TRADING else bot_stats.last_balance
                    max_exp = total_eq * MAX_EXPOSED_PERCENT
                    daily_limit = max_exp * MAX_DAILY_LOSS_PERCENT_OF_EXPOSED
                    if _daily_loss_tracker > daily_limit:
                        _set_paused(True)
                        daily_loss_cooldown_until = datetime.now() + timedelta(hours=COOLDOWN_HOURS_LOSS_LIMIT)
                        send_telegram(f"🚨 توقف بسبب الخسارة اليومية: {_daily_loss_tracker:.2f}")
                        generate_error_report("خطأ_حرج", "مراقبة", f"توقف للخسارة اليومية: {_daily_loss_tracker:.2f}")
                else:
                    if not is_sim:
                        new_stop = None
                        if pos.trailing_activated and pos.trailing_stop is not None: new_stop = pos.trailing_stop
                        else:
                            # استخدام النسبة الموحدة للتفعيل
                            new_stop = pos.entry_price * (1 - PREPLACED_SL_TRIGGER_PERCENT)
                        if new_stop: update_preemptive_stop_loss(symbol, pos, new_stop)
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
        ticker = fetch_ticker_with_retry(sym)
        if ticker and ticker.get('last'):
            close_partial(sym, 1.0, ticker['last'], reason)
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
                ticker = fetch_ticker_with_retry(sym)
                if not ticker: continue
                cur_price = ticker['last']
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

def get_current_momentum(symbol, lookback_minutes):
    try:
        df = fetch_ohlcv_retry(symbol, '5m', limit=50)
        if len(df) < 4: return None
        lookback = max(1, int(lookback_minutes / 5))
        if len(df) < lookback + 2: return None
        price_now = df['close'].iloc[-1]
        price_past = df['close'].iloc[-lookback-1]
        return (price_now - price_past) / price_past
    except Exception: return None

def monitor_positions():
    global _last_market_condition_time, _last_market_condition
    selling_comm = SellingCommittee()
    while True:
        try:
            with _global_state_lock: snapshot = list(open_positions.items())
            all_tickers = fetch_tickers_with_retry()
            if not all_tickers: time.sleep(3); continue
            with _market_condition_lock:
                if time.time() - _last_market_condition_time > 120:
                    _last_market_condition = get_market_condition()
                    _last_market_condition_time = time.time()
                current_market = _last_market_condition
            for sym, pos in snapshot:
                ticker = all_tickers.get(sym)
                if not ticker: continue
                cur_price = ticker.get('last')
                if cur_price is None or cur_price <= 0: continue
                tf = SCALP_TIMEFRAME if pos.symbol_type=='meme' else TIMEFRAMES['primary']
                df = get_cached_features(sym, tf, limit=100, ttl=30)
                if df.empty: continue
                profit = pos.update(cur_price)
                if profit < 0:
                    if profit <= -STOP_LOSS_PARTIAL_1_PERCENT and not pos.sold_at_15:
                        close_partial(sym, 0.25, cur_price, f"⚠️ بيع وقائي: خسارة {abs(profit):.2%} (بيع 25%) [عتبة {STOP_LOSS_PARTIAL_1_PERCENT:.2%}]")
                        pos.sold_at_15 = True
                        save_state()
                    elif profit <= -STOP_LOSS_PARTIAL_2_PERCENT and not pos.sold_at_20:
                        sell_percent = 0.33
                        close_partial(sym, sell_percent, cur_price, f"⚠️ بيع وقائي: خسارة {abs(profit):.2%} (بيع 33%) [عتبة {STOP_LOSS_PARTIAL_2_PERCENT:.2%}]")
                        pos.sold_at_20 = True
                        save_state()
                    elif profit <= -STOP_LOSS_FULL_PERCENT:
                        if pos.remaining_size > 0:
                            close_partial(sym, 1.0, cur_price, f"💀 بيع وقائي كامل: خسارة {abs(profit):.2%} (إغلاق كامل) [عتبة {STOP_LOSS_FULL_PERCENT:.2%}]")
                if pos.crash_monitor_start is None:
                    pos.crash_monitor_start = datetime.now()
                    pos.lowest_drop = 0.0
                else:
                    drop = (pos.entry_price - cur_price) / pos.entry_price if pos.side == 'buy' else (cur_price - pos.entry_price) / pos.entry_price
                    if drop > pos.lowest_drop: pos.lowest_drop = drop
                    elapsed_seconds = (datetime.now() - pos.crash_monitor_start).total_seconds()
                    if elapsed_seconds <= 120 and drop >= 0.04:
                        close_partial(sym, 1.0, cur_price, "💥 انهيار مفاجئ >4% خلال دقيقتين (إغلاق كامل)")
                        continue
                sell_dec, avg, details = selling_comm.decide_sell(df, sym, pos, cur_price, market_condition=current_market)
                if sell_dec:
                    close_partial(sym, 1.0, cur_price, f"🗳️ قرار لجنة البيع (متوسط {avg:.2%}) [عتبة البيع {SELL_DECISION_THRESHOLD:.2%}]")
                    continue
                if pos.trailing_activated and pos.trailing_stop is not None:
                    if pos.last_trailing_stop_sent is None:
                        should_update = True
                    else:
                        increase_ratio = (pos.trailing_stop - pos.last_trailing_stop_sent) / pos.last_trailing_stop_sent
                        should_update = increase_ratio >= 0.01
                    if should_update:
                        update_preemptive_stop_loss(sym, pos, pos.trailing_stop)
                if pos.side == 'buy' and (cur_price - pos.entry_price) / pos.entry_price <= -0.06:
                    if (datetime.now() - pos.open_time).total_seconds() < 30:
                        close_partial(sym, 1.0, cur_price, "🚨 انهيار سريع >6%")
                        continue
                if profit > 0.011:
                    now_time = datetime.now()
                    if pos.last_target_hit_index == -1:
                        if (now_time - pos.open_time).total_seconds() > 3600:
                            first_target = pos.take_profit_levels[0][0] if pos.take_profit_levels else None
                            if first_target and cur_price < first_target:
                                close_partial(sym, 1.0, cur_price, "🕒 انقضت ساعة دون تحقيق الهدف الأول – جني أرباح")
                                continue
                    else:
                        next_target_idx = pos.last_target_hit_index + 1
                        if next_target_idx < len(pos.take_profit_levels):
                            next_target = pos.take_profit_levels[next_target_idx][0]
                            if (now_time - pos.last_target_hit_time).total_seconds() > 3600 and cur_price < next_target:
                                close_partial(sym, 1.0, cur_price, f"🕒 انقضت ساعة بعد تحقيق الهدف {pos.last_target_hit_index+1} دون الوصول للهدف التالي – جني أرباح")
                                continue
                tp_tolerance = 0.001
                tp_levels_copy = list(pos.take_profit_levels)
                executed = False
                for idx, (target, pct) in enumerate(tp_levels_copy):
                    if (pos.side == 'buy' and cur_price >= target * (1 - tp_tolerance)) or (pos.side == 'sell' and cur_price <= target * (1 + tp_tolerance)):
                        pos.last_target_hit_time = datetime.now()
                        pos.last_target_hit_index = idx
                        if idx >= 5:
                            close_partial(sym, 1.0, cur_price, f"🎯 جني أرباح كامل (الهدف {idx+1})")
                        else:
                            close_partial(sym, pct, cur_price, f"🎯 جني أرباح جزئي (الهدف {idx+1})")
                        if [target, pct] in pos.take_profit_levels: pos.take_profit_levels.remove([target, pct])
                        executed = True
                        break
                if executed: continue
                if pos.trailing_activated and pos.trailing_stop:
                    if (pos.side=='buy' and cur_price <= pos.trailing_stop) or (pos.side=='sell' and cur_price >= pos.trailing_stop):
                        close_partial(sym, 1.0, cur_price, "🏃 وقف متحرك")
                        continue
                if pos.stop_loss:
                    sl = pos.stop_loss * (0.997 if pos.side=='buy' else 1.003)
                    if (pos.side=='buy' and cur_price <= sl) or (pos.side=='sell' and cur_price >= sl):
                        close_partial(sym, 1.0, cur_price, "🛑 وقف خسارة")
                        continue
                if profit <= -0.06:
                    close_partial(sym, 1.0, cur_price, "💥 أقصى خسارة 6%")
                    continue
            time.sleep(3)
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

def execute_full_trade(symbol, side, size, price, atr, sl, tp, sym_type, pred, conf, regime):
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
            pos = Position(symbol, side, size, price, atr, sl, tp, sym_type, pred, conf, regime)
            with _global_state_lock: open_positions[symbol] = pos
            send_telegram(f"🧪 [{'محاكاة' if TEST_MODE else 'ورقي'}] شراء {symbol} ({sym_type}) | الحجم: {size:.6f} | السعر: {price:.8f}")
            save_state()
            return True, size
        success, fill_price, filled_size, _ = execute_limit_order(symbol, side, size, price, sym_type)
        if success and fill_price and filled_size>0:
            pos = Position(symbol, side, filled_size, fill_price, atr, sl, tp, sym_type, pred, conf, regime)
            sl_order_id = place_preemptive_stop_loss(symbol, filled_size, fill_price, sym_type)
            if not sl_order_id:
                logger.error(f"❌ فشل إنشاء وقف مسبق لـ {symbol}. جاري إلغاء الصفقة...")
                if not _should_simulate():
                    try:
                        exchange = get_active_exchange()
                        close_side = 'sell' if side == 'buy' else 'buy'
                        exchange.create_market_order(symbol, close_side, filled_size)
                        logger.info(f"✅ تم إلغاء صفقة {symbol} بسبب فشل الوقف المسبق")
                        send_telegram(f"⚠️ تم إلغاء صفقة {symbol}: فشل إنشاء وقف الخسارة")
                    except Exception as e:
                        logger.error(f"⚠️ فشل إلغاء الصفقة {symbol}: {e}")
                        send_telegram(f"🚨 تنبيه: صفقة {symbol} مفتوحة بدون وقف خسارة! تدخل يدوي مطلوب.")
                return False, 0
            pos.preemptive_sl_order_id = sl_order_id
            with _global_state_lock: open_positions[symbol] = pos
            new_balance = _safe_fetch_balance_after_trade(attempts=10, delay=3.0, silent=True)
            if new_balance is not None:
                with _global_state_lock: bot_stats.last_balance = new_balance
            send_telegram(f"✅ <b>شراء</b> {symbol} ({sym_type}) | الحجم: {filled_size:.6f} | السعر: {fill_price:.8f}")
            save_state()
            return True, filled_size
        else: return False, 0
    except Exception as e:
        logger.error(f"خطأ غير متوقع في execute_full_trade لـ {symbol}: {e}")
        generate_error_report("فشل_أمر", "تنفيذ", str(e))
        return False, 0
    finally:
        with _global_state_lock: _local_pending_symbols.discard(symbol)

# --------------------------- خيط auto_recovery الجديد ---------------------------
def auto_recovery_monitor():
    """خيط مستقل لمراقبة الاتصال بـ Binance وإعادة ضبط البوت تلقائياً عند الحظر المتكرر."""
    global _auto_recovery_failures, rest_rate_limiter
    exchange = get_active_exchange()
    test_symbol = 'BTC/USDT'
    while True:
        try:
            # محاولة جلب تيكر بسيط (بدون معامل timeout خاطئ)
            ticker = exchange.fetch_ticker(test_symbol)
            if ticker and ticker.get('last') is not None:
                with _auto_recovery_lock:
                    if _auto_recovery_failures > 0:
                        logger.info("✅ استعاد البوت الاتصال بـ Binance تلقائياً.")
                        _auto_recovery_failures = 0
        except Exception as e:
            logger.warning(f"⚠️ فشل اختبار الاتصال: {e}")
            with _auto_recovery_lock:
                _auto_recovery_failures += 1
                failures = _auto_recovery_failures
            if failures >= _AUTO_RECOVERY_THRESHOLD:
                logger.critical(f"❌ {_AUTO_RECOVERY_THRESHOLD} محاولات فاشلة متتالية - بدء إجراء الاسترداد التلقائي...")
                send_telegram("🔄 اكتشاف حظر من Binance - جاري الاسترداد التلقائي (إعادة ضبط الـ RateLimiter و WebSocket).")
                try:
                    # 1. إعادة تعيين RateLimiter (إنشاء كائن جديد)
                    rest_rate_limiter = RateLimiter(max_calls=50, period=60)
                    logger.info("تم إعادة تعيين RateLimiter.")

                    # 2. إعادة تشغيل WebSocket Manager
                    ws_manager.restart()
                    logger.info("تم إعادة تشغيل WebSocket Manager.")

                    # 3. مسح ذاكرة التخزين المؤقت
                    with _cache_lock:
                        _ohlcv_cache.clear()
                    with _features_cache_lock:
                        _features_cache.clear()
                    with _orderbook_cache_lock:
                        _orderbook_cache.clear()
                    with _trades_cache_lock:
                        _trades_cache.clear()
                    with _cache_lock:
                        _manipulation_cache.clear()
                    logger.info("تم مسح جميع ذاكرات التخزين المؤقت.")

                    # 4. إعادة تحميل الأسواق (محاولة خفيفة)
                    if load_markets_with_retry(max_retries=3, initial_delay=10):
                        logger.info("✅ تم إعادة تحميل الأسواق بنجاح.")
                        send_telegram("✅ الاسترداد التلقائي ناجح. البوت يعمل مجدداً.")
                        _auto_recovery_failures = 0
                    else:
                        logger.error("❌ فشل إعادة تحميل الأسواق أثناء الاسترداد. سأحاول مجدداً لاحقاً.")
                        send_telegram("⚠️ فشل إعادة تحميل الأسواق أثناء الاسترداد التلقائي. قد تحتاج إلى تدخل يدوي.")
                except Exception as rec_err:
                    logger.error(f"خطأ أثناء الاسترداد التلقائي: {rec_err}")
                    send_telegram(f"⚠️ فشل الاسترداد التلقائي: {rec_err}")
                # إعادة تعيين العداد حتى لا نكرر المحاولات بسرعة كبيرة
                with _auto_recovery_lock:
                    _auto_recovery_failures = 0
        # انتظر 60 ثانية بين كل فحص
        time.sleep(60)

# --------------------------- دالة التحليل الرئيسية ---------------------------
def analyze_and_trade():
    global bot_stats, _daily_loss_tracker, _daily_trades_count
    global _daily_trades_date, _daily_winning_trades, _daily_losing_trades
    global daily_loss_cooldown_until
    global _initial_warmup_done
    global _last_processing_lock_released
    global _last_market_condition, _last_market_condition_time
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    global _daily_biggest_win, _daily_biggest_loss, _daily_most_traded
    global buying_committee, _analysis_failures

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

        if not _initial_warmup_done and elapsed_since_start < STARTUP_DELAY_SECONDS + 30:
            logger.info("🔥 تحميل الكاش الأولي للرموز الأساسية...")
            for sym in BASE_SYMBOLS:
                for tf in [TIMEFRAMES['primary'], TIMEFRAMES['confirm_1'], TIMEFRAMES['confirm_2']]:
                    get_cached_features(sym, tf, limit=500, ttl=90)
                    time.sleep(0.5)
            _initial_warmup_done = True
            logger.info("✅ تم تحميل الكاش الأولي. بدء التحليل العادي بعد قليل.")
            return

        effective_limit = ACTIVE_SYMBOLS_LIMIT
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
        free_bal = get_real_balance_usdt(max_retries=3, delay=2, silent=True) if not TEST_MODE and not PAPER_TRADING else bot_stats.last_balance
        if free_bal is None or free_bal < 10: return
        try: market_cond = get_market_condition()
        except Exception: market_cond = 'normal'
        with _market_condition_lock:
            _last_market_condition = market_cond
            _last_market_condition_time = time.time()
        logger.info(f"حالة السوق: {market_cond}")
        position_percent = MAX_POSITION_PERCENT_GOOD if market_cond == 'good' else MAX_POSITION_PERCENT
        max_pos_usdt = total_eq * position_percent
        if market_cond == 'good': cooldown_hours = COOLDOWN_HOURS_GOOD
        elif market_cond == 'bad': cooldown_hours = COOLDOWN_HOURS_BAD
        else: cooldown_hours = BASE_COOLDOWN_HOURS
        if scanner.last_scan == 0 and elapsed_since_start < INITIAL_SCAN_DELAY:
            logger.info(f"⏳ تأجيل أول مسح للسوق لمدة {INITIAL_SCAN_DELAY - elapsed_since_start:.0f} ثانية")
        elif scanner.should_scan(): scanner.scan()
        all_syms = list(set(BASE_SYMBOLS + scanner.candidates + list(open_positions.keys())))
        active = all_syms[:effective_limit]
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
        max_allowed_exposure = total_eq * 0.70 if market_cond == 'good' else total_eq * MAX_EXPOSED_PERCENT
        buy_comm = buying_committee
        with _global_state_lock: forbidden = set(open_positions.keys()).union(_local_pending_symbols).union(_exchange_pending_symbols)
        candidates_for_manipulation = []
        for sym in active:
            if sym in forbidden: continue
            if last_close_time.get(sym) and (datetime.now() - last_close_time[sym]).total_seconds() < cooldown_hours * 3600: continue
            regime = detect_market_regime(sym)
            if regime == 'trending_down': continue
            df_primary = fetch_ohlcv_persistent(sym, TIMEFRAMES['primary'], limit=500, max_attempts=5, retry_interval=10)
            if df_primary.empty or len(df_primary) < 50: continue
            time.sleep(0.3)
            sym_type = classify_symbol(sym, df=df_primary, ticker_data=all_tickers.get(sym))
            candidates_for_manipulation.append((sym, sym_type, df_primary, regime))
        manipulation_results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_manipulation, sym, sym_type): sym for sym, sym_type, _, _ in candidates_for_manipulation}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    is_manip, score, m_type = future.result(timeout=MANIPULATION_TIMEOUT)
                    manipulation_results[sym] = (is_manip, score, m_type)
                except TimeoutError: manipulation_results[sym] = (False, 0.0, "timeout")
                except Exception: manipulation_results[sym] = (False, 0.0, "error")
        pending = []
        for sym, sym_type, df_primary, regime in candidates_for_manipulation:
            if FILTER_MANIPULATION_ENABLED:
                is_manipulated, suspicion_score, manip_type = manipulation_results.get(sym, (False, 0.0, "missing"))
                if is_manipulated:
                    logger.info(f"🚫 رفض الشراء لـ {sym}: تم اكتشاف تلاعب ({manip_type}) درجة {suspicion_score:.2%} (العتبة {MANIPULATION_REJECT_THRESHOLD:.2%})")
                    if manip_type not in _last_error_report_times or time.time() - _last_error_report_times.get(manip_type, 0) > 300:
                        send_telegram(f"⚠️ <b>تم منع الشراء</b>\nالرمز: {sym}\nالسبب: {manip_type}\nدرجة الاشتباه: {suspicion_score:.2%}\nالعتبة الحالية: {MANIPULATION_REJECT_THRESHOLD:.2%}")
                        _last_error_report_times[manip_type] = time.time()
                    continue
            dec, score, conf = buy_comm.decide(df_primary, sym, sym_type, market_condition=market_cond)
            if dec != 'buy': continue
            if (sym_type == 'meme' and score < SCALP_MIN_PROFIT) or (sym_type != 'meme' and score < STRENGTH_THRESHOLD): continue
            ticker = all_tickers.get(sym)
            if not ticker: continue
            cur_price = ticker.get('last')
            if cur_price is None or cur_price <= 0: continue
            if FILTER_HOUR_CANDLE_ENABLED:
                df_1h = fetch_ohlcv_persistent(sym, '1h', limit=2, max_attempts=5, retry_interval=10)
                if df_1h.empty or len(df_1h) < 2: continue
                last_completed_close = df_1h['close'].iloc[-2]
                if cur_price <= last_completed_close: continue
            if FILTER_4H_CANDLE_ENABLED:
                df_4h = fetch_ohlcv_persistent(sym, '4h', limit=2, max_attempts=5, retry_interval=10)
                if df_4h.empty or len(df_4h) < 2: continue
                last_completed_close_4h = df_4h['close'].iloc[-2]
                if cur_price <= last_completed_close_4h: continue
            atr_val = df_primary['atr'].iloc[-1] if 'atr' in df_primary and not df_primary['atr'].isna().all() else cur_price * 0.02
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
            extra = score * 100 * 0.35 + conf * 30 + (2 if sym_type == 'meme' else 0)
            pending.append({'symbol': sym, 'score': extra, 'price': cur_price, 'atr': atr_val,
                            'sl': sl_price, 'tp': tp_price, 'size': size, 'type': sym_type,
                            'pred': score, 'conf': conf, 'regime': regime})
        if pending:
            pending.sort(key=lambda x: x['score'], reverse=True)
            rem_bal = free_bal
            exp = curr_exp
            for opp in pending:
                with _global_state_lock:
                    if _daily_trades_count >= MAX_DAILY_TRADES: break
                    if opp['symbol'] in open_positions or opp['symbol'] in _local_pending_symbols or opp['symbol'] in _exchange_pending_symbols: continue
                total_eq2 = get_total_equity() if not TEST_MODE and not PAPER_TRADING and ENABLE_TRADING else bot_stats.last_balance
                max_exp2 = total_eq2 * 0.70 if market_cond == 'good' else total_eq2 * MAX_EXPOSED_PERCENT
                pos_usdt = total_eq2 * (MAX_POSITION_PERCENT_GOOD if market_cond == 'good' else MAX_POSITION_PERCENT)
                sz = pos_usdt / opp['price']
                min_amt2, max_amt2, min_cost2 = get_amount_limits(opp['symbol'])
                if sz < min_amt2: continue
                if max_amt2 and sz > max_amt2: sz = max_amt2
                if min_cost2 and sz * opp['price'] < min_cost2: continue
                if exp + pos_usdt > max_exp2: continue
                if rem_bal < pos_usdt: continue
                succ, actual_size = execute_full_trade(opp['symbol'], 'buy', sz, opp['price'], opp['atr'],
                                                       opp['sl'], opp['tp'], opp['type'], opp['pred'],
                                                       opp['conf'], opp['regime'])
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
        rest_rate_limiter.wait_if_needed()
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
        time.sleep(15)

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
        if scanner: scanner.scan()
        time.sleep(SCAN_INTERVAL_MINUTES * 60)

def background_analyzer():
    time.sleep(STARTUP_DELAY_SECONDS)
    while True:
        analyze_and_trade()
        time.sleep(100)

def background_balance_updater():
    time.sleep(30)
    while True:
        if ENABLE_TRADING and not TEST_MODE and not PAPER_TRADING:
            get_real_balance_usdt(max_retries=3, delay=2, silent=True)
            get_total_equity()
        time.sleep(60)

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
        with _cache_lock:
            items = list(_manipulation_cache.items())
            if len(items) > 400:
                for key, _ in items[:50]:
                    if key in _manipulation_cache: del _manipulation_cache[key]
        if now - last_full_cleanup > 1800:
            last_full_cleanup = now
            cleanup_error_reports()
            with _trades_cache_lock:
                items = list(_trades_cache.items())
                if len(items) > 200:
                    for key, _ in items[:50]: del _trades_cache[key]
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
            if memory_mb > 1024:
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

def sync_preemptive_stop_orders():
    while True:
        try:
            if TEST_MODE or PAPER_TRADING or not ENABLE_TRADING: time.sleep(60); continue
            exchange = get_active_exchange()
            with _global_state_lock: symbols_to_check = list(open_positions.keys())
            for sym in symbols_to_check:
                try:
                    rest_rate_limiter.wait_if_needed()
                    orders = exchange.fetch_open_orders(symbol=sym)
                    stop_orders = [o for o in orders if o.get('type') == 'stop_loss_limit']
                    order_map = {o['symbol']: o['id'] for o in stop_orders} if stop_orders else {}
                    with _global_state_lock:
                        pos = open_positions.get(sym)
                        if not pos: continue
                        current_oid = pos.preemptive_sl_order_id
                        if current_oid and current_oid not in order_map.values():
                            logger.warning(f"⚠️ أمر الوقف المسبق {current_oid} لـ {sym} غير موجود على المنصة. سنرسل أمراً جديداً.")
                            if pos.trailing_activated and pos.trailing_stop is not None:
                                new_stop = pos.trailing_stop
                            else:
                                new_stop = pos.entry_price * (1 - PREPLACED_SL_TRIGGER_PERCENT)
                            new_oid = place_preemptive_stop_loss(sym, pos.remaining_size, pos.entry_price, pos.symbol_type, stop_price_override=new_stop)
                            if new_oid:
                                pos.preemptive_sl_order_id = new_oid
                                pos.last_trailing_stop_sent = new_stop
                        elif not current_oid and sym not in order_map:
                            if pos.trailing_activated and pos.trailing_stop is not None:
                                new_stop = pos.trailing_stop
                            else:
                                new_stop = pos.entry_price * (1 - PREPLACED_SL_TRIGGER_PERCENT)
                            new_oid = place_preemptive_stop_loss(sym, pos.remaining_size, pos.entry_price, pos.symbol_type, stop_price_override=new_stop)
                            if new_oid:
                                pos.preemptive_sl_order_id = new_oid
                                pos.last_trailing_stop_sent = new_stop
                        elif sym in order_map and current_oid != order_map.get(sym):
                            logger.info(f"🔄 تحديث معرف الوقف المسبق لـ {sym} من {current_oid} إلى {order_map[sym]}")
                            pos.preemptive_sl_order_id = order_map[sym]
                except Exception as e:
                    logger.warning(f"فشل مزامنة الوقف المسبق لـ {sym}: {e}")
        except Exception as e:
            logger.error(f"خطأ في sync_preemptive_stop_orders: {e}")
        time.sleep(60)

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
            report = (f"<b>📊 تقرير دوري (كل 4 ساعات)</b>\n🕒 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n💰 الرصيد الكلي: ${total_equity:.2f}\n📈 إجمالي الربح الصافي: ${total_pnl:.2f}\n📊 صفقات اليوم: {daily_trades}\n🔓 مراكز مفتوحة: {positions_count}\n⏸️ متوقف: {'نعم' if paused else 'لا'}\n🎯 عتبة الاشتباه الحالية: {MANIPULATION_REJECT_THRESHOLD:.2%}\n🎯 عتبة البيع الحالية: {SELL_DECISION_THRESHOLD:.2%}\n🛑 عتبات الخسارة الوقائية:\n   - بيع 25% عند {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n   - بيع 33% عند {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n   - بيع كامل عند {STOP_LOSS_FULL_PERCENT:.2%}")
            if positions_count > 0:
                positions_detail = ""
                for sym, pos in open_positions.items():
                    ticker = fetch_ticker_with_retry(sym)
                    price = ticker['last'] if ticker else pos.entry_price
                    profit = (price - pos.entry_price) / pos.entry_price if pos.side == 'buy' else (pos.entry_price - price) / pos.entry_price
                    positions_detail += f"• {sym} ({pos.side}) ربح: {profit:.2%}\n"
                report += f"<b>📋 المراكز المفتوحة:</b>\n{positions_detail}"
            else: report += "لا توجد مراكز مفتوحة."
            send_telegram(report)
        except Exception as e: logger.error(f"خطأ في التقرير الدوري: {e}")
        time.sleep(14400)

def telegram_polling():
    global _last_telegram_update_id
    global FILTER_MANIPULATION_ENABLED, FILTER_LIQUIDITY_ENABLED, FILTER_MARKET_CAP_ENABLED
    global FILTER_VOLUME_24H_ENABLED, FILTER_CHANGE_24H_ENABLED, FILTER_HOUR_CANDLE_ENABLED, FILTER_4H_CANDLE_ENABLED
    global BASE_COOLDOWN_HOURS, COOLDOWN_HOURS_GOOD, COOLDOWN_HOURS_BAD
    global STRENGTH_THRESHOLD, SCALP_MIN_PROFIT, SCAN_INTERVAL_MINUTES
    global PAPER_TRADING, TEST_MODE, bot_stats, open_positions
    global _daily_loss_tracker, _daily_trades_count, _daily_winning_trades, _daily_losing_trades
    global _daily_biggest_win, _daily_biggest_loss, _daily_most_traded
    global _daily_total_holding_time_win, _daily_total_holding_time_loss, _daily_holding_count_win, _daily_holding_count_loss
    global MANIPULATION_REJECT_THRESHOLD, SELL_DECISION_THRESHOLD
    global STOP_LOSS_PARTIAL_1_PERCENT, STOP_LOSS_PARTIAL_2_PERCENT, STOP_LOSS_FULL_PERCENT
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
                            if text.startswith('تعيين عتبة خسارة 25%'):
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
                            elif text == 'عتبات الخسارة':
                                send_telegram(f"🛑 عتبات الخسارة الوقائية الحالية:\n- بيع 25% عند خسارة {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n- بيع 33% عند خسارة {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n- بيع كامل عند خسارة {STOP_LOSS_FULL_PERCENT:.2%}")
                            elif text.startswith('رفع عتبة الاشتباه'):
                                match = re.search(r'(\d+(?:\.\d+)?)%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.1 <= new_percent <= 0.95:
                                        MANIPULATION_REJECT_THRESHOLD = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم رفع عتبة الاشتباه إلى {new_percent:.2%}\n(سيتم رفض العملات التي تتجاوز درجة الاشتباه هذه)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 10% و 95%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `رفع عتبة الاشتباه 69%`")
                            elif text.startswith('خفض عتبة الاشتباه'):
                                match = re.search(r'(\d+(?:\.\d+)?)%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.1 <= new_percent <= 0.95:
                                        MANIPULATION_REJECT_THRESHOLD = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم خفض عتبة الاشتباه إلى {new_percent:.2%}\n(سيتم رفض العملات التي تتجاوز درجة الاشتباه هذه)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 10% و 95%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `خفض عتبة الاشتباه 60%`")
                            elif text == 'عتبة الاشتباه':
                                send_telegram(f"🎯 عتبة الاشتباه الحالية: {MANIPULATION_REJECT_THRESHOLD:.2%}")
                            elif text.startswith('رفع عتبة البيع'):
                                match = re.search(r'(\d+(?:\.\d+)?)%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.3 <= new_percent <= 0.9:
                                        SELL_DECISION_THRESHOLD = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم رفع عتبة البيع إلى {new_percent:.2%}\n(لجنة البيع ستبيع عندما يتجاوز متوسط الأصوات هذه القيمة)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 30% و 90%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `رفع عتبة البيع 70%`")
                            elif text.startswith('خفض عتبة البيع'):
                                match = re.search(r'(\d+(?:\.\d+)?)%?', text)
                                if match:
                                    new_percent = float(match.group(1)) / 100.0
                                    if 0.3 <= new_percent <= 0.9:
                                        SELL_DECISION_THRESHOLD = new_percent
                                        save_state()
                                        send_telegram(f"✅ تم خفض عتبة البيع إلى {new_percent:.2%}\n(لجنة البيع ستبيع عندما يتجاوز متوسط الأصوات هذه القيمة)")
                                    else:
                                        send_telegram("⚠️ القيمة يجب أن تكون بين 30% و 90%")
                                else:
                                    send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `خفض عتبة البيع 55%`")
                            elif text == 'عتبة البيع':
                                send_telegram(f"🎯 عتبة البيع الحالية: {SELL_DECISION_THRESHOLD:.2%}")
                            elif text == "توقف عن التداول":
                                _set_paused(True); close_all_positions("أمر تلغرام"); send_telegram("⏸️ تم الإيقاف وإغلاق الصفقات"); save_state()
                            elif text == "تابع التداول": manual_resume()
                            elif text == "صفقاتي" or text == "صفقاتي":
                                with _global_state_lock: positions = list(open_positions.items())
                                if not positions: send_telegram("📭 لا توجد صفقات مفتوحة حالياً.")
                                else:
                                    total_value = 0.0; total_pnl = 0.0; lines = ["<b>📊 تقرير الصفقات المفتوحة</b>"]
                                    current_prices = {}
                                    for sym, _ in positions:
                                        ticker = fetch_ticker_with_retry(sym)
                                        if ticker and ticker.get('last'): current_prices[sym] = ticker['last']
                                        else: current_prices[sym] = None
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
                                    if PAPER_TRADING or TEST_MODE: free_balance = bot_stats.last_balance; mode_text = "ورقي (محاكاة)"
                                    else:
                                        real_bal = get_real_balance_usdt(max_retries=3, delay=2, silent=False)
                                        if real_bal is None: free_balance = bot_stats.last_balance; mode_text = "حقيقي (بيانات مخزنة - قد لا تكون محدثة)"
                                        else: free_balance = real_bal; mode_text = "حقيقي"
                                    positions_value = 0.0; positions_details = []
                                    with _global_state_lock:
                                        for sym, pos in open_positions.items():
                                            ticker = fetch_ticker_with_retry(sym)
                                            current_price = ticker['last'] if ticker and ticker.get('last') else pos.entry_price
                                            value = pos.remaining_size * current_price
                                            positions_value += value
                                            profit_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100 if pos.side == 'buy' else ((pos.entry_price - current_price) / pos.entry_price) * 100
                                            positions_details.append(f"• {sym} ({pos.side}) {pos.remaining_size:.6f} @ {current_price:.8f} ≈ ${value:.2f} (ربح {profit_pct:+.2f}%)")
                                    total = free_balance + positions_value
                                    msg = f"<b>💰 رصيدك الحالي ({mode_text})</b>\n━━━━━━━━━━━━━━━━━━━━\n💵 <b>الرصيد الحر:</b> ${free_balance:,.2f}\n"
                                    if positions_details:
                                        msg += f"\n📊 <b>المراكز المفتوحة ({len(positions_details)})</b>:\n" + "\n".join(positions_details) + f"\n\n💼 <b>قيمة المراكز:</b> ${positions_value:,.2f}\n"
                                    else: msg += f"\n📭 لا توجد مراكز مفتوحة.\n"
                                    msg += f"━━━━━━━━━━━━━━━━━━━━\n🏦 <b>الإجمالي الكلي:</b> ${total:,.2f}"
                                    send_telegram(msg)
                                except Exception as e: logger.error(f"خطأ في جلب الرصيد: {e}"); send_telegram("⚠️ حدث خطأ أثناء جلب الرصيد. تأكد من الاتصال بالمنصة.")
                            elif text == 'حالة الفلاتر':
                                status = (f"📊 <b>حالة الفلاتر والقيم الحالية</b>\n━━━━━━━━━━━━━━━━━━━━\n🛡️ <b>فلتر التلاعب:</b> {'✅ مفعل' if FILTER_MANIPULATION_ENABLED else '❌ معطل'}\n💧 <b>فلتر السيولة:</b> {'✅ مفعل' if FILTER_LIQUIDITY_ENABLED else '❌ معطل'}\n💰 <b>فلتر القيمة السوقية:</b> {'✅ مفعل' if FILTER_MARKET_CAP_ENABLED else '❌ معطل'} (حد: ${MIN_MARKET_CAP_USD:,.0f})\n📊 <b>فلتر حجم التداول 24ساعة:</b> {'✅ مفعل' if FILTER_VOLUME_24H_ENABLED else '❌ معطل'} (حد: ${MIN_24H_VOLUME_USD:,.0f})\n📈 <b>فلتر التغيير 24ساعة:</b> {'✅ مفعل' if FILTER_CHANGE_24H_ENABLED else '❌ معطل'} (حد: {MIN_24H_CHANGE_PERCENT:.1f}%)\n🕐 <b>فلتر شمعة الساعة:</b> {'✅ مفعل' if FILTER_HOUR_CANDLE_ENABLED else '❌ معطل'}\n🕓 <b>فلتر شمعة 4 ساعات:</b> {'✅ مفعل' if FILTER_4H_CANDLE_ENABLED else '❌ معطل'}\n⏱️ <b>فترة التبريد:</b> {BASE_COOLDOWN_HOURS:.1f} ساعة\n━━━━━━━━━━━━━━━━━━━━\n⚙️ <b>عتبات لجنة الشراء:</b> مضاعف {CURRENT_BUY_COMMITTEE_MULTIPLIER:.2f}×\n🎯 <b>عتبة شراء عادي:</b> {STRENGTH_THRESHOLD:.2f}\n🎯 <b>عتبة شراء ميم:</b> {SCALP_MIN_PROFIT:.2f}\n🎯 <b>عتبة الاشتباه (التلاعب):</b> {MANIPULATION_REJECT_THRESHOLD:.2%}\n🎯 <b>عتبة البيع:</b> {SELL_DECISION_THRESHOLD:.2%}\n🛑 <b>عتبات الخسارة الوقائية:</b>\n   - بيع 25% عند {STOP_LOSS_PARTIAL_1_PERCENT:.2%}\n   - بيع 33% عند {STOP_LOSS_PARTIAL_2_PERCENT:.2%}\n   - بيع كامل عند {STOP_LOSS_FULL_PERCENT:.2%}\n🔄 <b>مدة المسح:</b> {SCAN_INTERVAL_MINUTES} دقيقة")
                                send_telegram(status)
                            elif text == 'اعادة ضبط الفلاتر':
                                FILTER_MANIPULATION_ENABLED = True; FILTER_LIQUIDITY_ENABLED = True; FILTER_MARKET_CAP_ENABLED = True; FILTER_VOLUME_24H_ENABLED = True; FILTER_CHANGE_24H_ENABLED = True; FILTER_HOUR_CANDLE_ENABLED = True; FILTER_4H_CANDLE_ENABLED = True
                                BASE_COOLDOWN_HOURS = 1.0; COOLDOWN_HOURS_GOOD = 0.5; COOLDOWN_HOURS_BAD = 1.0
                                STRENGTH_THRESHOLD = 0.25; SCALP_MIN_PROFIT = 0.20
                                buying_committee.apply_multiplier(1.0)
                                MANIPULATION_REJECT_THRESHOLD = 0.65
                                SELL_DECISION_THRESHOLD = 0.65
                                STOP_LOSS_PARTIAL_1_PERCENT = 0.008
                                STOP_LOSS_PARTIAL_2_PERCENT = 0.012
                                STOP_LOSS_FULL_PERCENT = 0.017
                                send_telegram("🟢 تم إعادة ضبط جميع الفلاتر والعتبات إلى القيم الافتراضية الأصلية"); save_state()
                            elif text == 'اوقف فلتر التلاعب': FILTER_MANIPULATION_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر التلاعب (لن يتم رفض الصفقات بسبب التلاعب)"); save_state()
                            elif text == 'شغل فلتر التلاعب': FILTER_MANIPULATION_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر التلاعب (سيتم رفض الصفقات المشبوهة)"); save_state()
                            elif text == 'اوقف فلتر السيولة': FILTER_LIQUIDITY_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر السيولة (لن يتم فحص العمق والسبريد)"); save_state()
                            elif text == 'شغل فلتر السيولة': FILTER_LIQUIDITY_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر السيولة (سيتم فحص العمق والسبريد قبل الشراء)"); save_state()
                            elif text == 'اوقف فلتر القيمة السوقية': FILTER_MARKET_CAP_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر القيمة السوقية (لن يتم فحص الحد الأدنى للقيمة السوقية)"); save_state()
                            elif text == 'شغل فلتر القيمة السوقية': FILTER_MARKET_CAP_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر القيمة السوقية (سيتم فحص MIN_MARKET_CAP_USD)"); save_state()
                            elif text == 'اوقف فلتر حجم التداول 24ساعة': FILTER_VOLUME_24H_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر حجم التداول 24 ساعة (لن يتم فحص MIN_24H_VOLUME_USD)"); save_state()
                            elif text == 'شغل فلتر حجم التداول 24ساعة': FILTER_VOLUME_24H_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر حجم التداول 24 ساعة (سيتم فحص الحجم)"); save_state()
                            elif text == 'اوقف فلتر التغيير 24ساعة': FILTER_CHANGE_24H_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر التغيير 24 ساعة (لن يتم فحص MIN_24H_CHANGE_PERCENT)"); save_state()
                            elif text == 'شغل فلتر التغيير 24ساعة': FILTER_CHANGE_24H_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر التغيير 24 ساعة (سيتم فحص التغير)"); save_state()
                            elif text == 'اوقف فلتر الشمعة الصاعدة': FILTER_HOUR_CANDLE_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر شمعة الساعة الصاعدة (لن يتم فحص السعر > إغلاق آخر ساعة)"); save_state()
                            elif text == 'شغل فلتر الشمعة الصاعدة': FILTER_HOUR_CANDLE_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر شمعة الساعة الصاعدة (سيتم فحص السعر > إغلاق آخر ساعة)"); save_state()
                            elif text == 'اوقف فلتر الشمعة الصاعدة 4ساعة' or text == 'اوقف فلتر الشمعة الصاعدة 4 ساعة': FILTER_4H_CANDLE_ENABLED = False; send_telegram("🔴 تم إيقاف فلتر شمعة 4 ساعات الصاعدة (لن يتم فحص السعر > إغلاق آخر شمعة 4 ساعات)"); save_state()
                            elif text == 'شغل فلتر الشمعة الصاعدة 4 ساعة' or text == 'شغل فلتر الشمعة الصاعدة 4ساعة': FILTER_4H_CANDLE_ENABLED = True; send_telegram("🟢 تم تشغيل فلتر شمعة 4 ساعات الصاعدة (سيتم فحص السعر > إغلاق آخر شمعة 4 ساعات)"); save_state()
                            elif text.startswith('فلتر التبريد'):
                                match = re.search(r'(\d+(?:\.\d+)?)\s*(دقيقة|ساعة)', text)
                                if match:
                                    value = float(match.group(1)); unit = match.group(2)
                                    hours = value / 60.0 if unit == 'دقيقة' else value
                                    BASE_COOLDOWN_HOURS = hours; COOLDOWN_HOURS_GOOD = hours * 0.5; COOLDOWN_HOURS_BAD = hours * 1.5
                                    send_telegram(f"✅ تم ضبط فترة التبريد إلى {value} {unit}\n📌 تبريد عادي: {BASE_COOLDOWN_HOURS} ساعة\n📌 تبريد سوق جيد: {COOLDOWN_HOURS_GOOD} ساعة\n📌 تبريد سوق سيء: {COOLDOWN_HOURS_BAD} ساعة"); save_state()
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `فلتر التبريد 30 دقيقة` أو `فلتر التبريد 2 ساعة`")
                            elif text.startswith('ارفع عتبة لجنة الشراء'):
                                match = re.search(r'(\d+(?:\.\d+)?)%', text)
                                if match:
                                    percent = float(match.group(1))
                                    new_multiplier = CURRENT_BUY_COMMITTEE_MULTIPLIER * (1 + percent / 100.0)
                                    new_multiplier = max(0.5, min(2.0, new_multiplier))
                                    buying_committee.apply_multiplier(new_multiplier)
                                    send_telegram(f"✅ تم رفع عتبات لجنة الشراء بنسبة {percent}%\n📊 المضاعف الجديد: {new_multiplier:.2f} (أي عتبات ×{new_multiplier:.2f})")
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `ارفع عتبة لجنة الشراء 20%`")
                            elif text.startswith('خفض عتبة لجنة الشراء'):
                                match = re.search(r'(\d+(?:\.\d+)?)%', text)
                                if match:
                                    percent = float(match.group(1))
                                    new_multiplier = CURRENT_BUY_COMMITTEE_MULTIPLIER * (1 - percent / 100.0)
                                    new_multiplier = max(0.5, min(2.0, new_multiplier))
                                    buying_committee.apply_multiplier(new_multiplier)
                                    send_telegram(f"✅ تم خفض عتبات لجنة الشراء بنسبة {percent}%\n📊 المضاعف الجديد: {new_multiplier:.2f} (أي عتبات ×{new_multiplier:.2f})")
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `خفض عتبة لجنة الشراء 10%`")
                            elif text.startswith('عتبة شراء ميم'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_val = float(match.group(1))
                                    if 0.05 <= new_val <= 0.50: SCALP_MIN_PROFIT = new_val; send_telegram(f"✅ تم تغيير عتبة شراء العملات الميم إلى {new_val:.2f}"); save_state()
                                    else: send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.50")
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عتبة شراء ميم 0.25`")
                            elif text.startswith('عتبة شراء عادية'):
                                match = re.search(r'(\d+(?:\.\d+)?)', text)
                                if match:
                                    new_val = float(match.group(1))
                                    if 0.05 <= new_val <= 0.50: STRENGTH_THRESHOLD = new_val; send_telegram(f"✅ تم تغيير عتبة شراء العملات العادية إلى {new_val:.2f}"); save_state()
                                    else: send_telegram("⚠️ القيمة يجب أن تكون بين 0.05 و 0.50")
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `عتبة شراء عادية 0.30`")
                            elif text.startswith('امسح السوق كل'):
                                match = re.search(r'(\d+)\s*دقيقة', text)
                                if match:
                                    new_interval = int(match.group(1))
                                    if 1 <= new_interval <= 60: SCAN_INTERVAL_MINUTES = new_interval; send_telegram(f"✅ تم تغيير مدة المسح إلى {new_interval} دقيقة"); save_state()
                                    else: send_telegram("⚠️ يرجى إدخال قيمة بين 1 و 60 دقيقة")
                                else: send_telegram("⚠️ الصيغة غير صحيحة. استخدم: `امسح السوق كل 5 دقيقة`")
                            elif text == 'تداول حقيقي':
                                if not ENABLE_TRADING: send_telegram("⚠️ التداول غير مفعل في الإعدادات الأساسية (ENABLE_TRADING=false). لا يمكن التحويل.")
                                elif PAPER_TRADING:
                                    with _global_state_lock:
                                        if open_positions: send_telegram("🔄 جاري إغلاق جميع المراكز الورقية قبل التحويل...")
                                    close_all_positions("التحويل إلى التداول الحقيقي - إغلاق المراكز الورقية")
                                    time.sleep(2)
                                    with _global_state_lock:
                                        open_positions.clear()
                                        _daily_loss_tracker = 0.0; _daily_trades_count = 0; _daily_winning_trades = 0; _daily_losing_trades = 0
                                        _daily_biggest_win = 0.0; _daily_biggest_loss = 0.0; _daily_most_traded.clear()
                                        _daily_total_holding_time_win = 0.0; _daily_total_holding_time_loss = 0.0
                                        _daily_holding_count_win = 0; _daily_holding_count_loss = 0
                                        bot_stats.total_pnl_usdt = 0.0; bot_stats.winning_trades = 0; bot_stats.losing_trades = 0
                                        bot_stats.total_trades = 0; bot_stats.daily_pnl = 0.0; bot_stats.weekly_pnl = 0.0
                                        bot_stats.weekly_wins = 0; bot_stats.weekly_losses = 0
                                    PAPER_TRADING = False; TEST_MODE = False
                                    send_telegram("⏳ جاري الاتصال بـ Binance لجلب الرصيد الحقيقي (قد يستغرق حتى دقيقتين)...")
                                    real_balance = fetch_real_balance_with_retry(timeout_seconds=120, retry_interval=5, silent=False)
                                    if real_balance is not None:
                                        with _global_state_lock: bot_stats.last_balance = real_balance
                                        send_telegram(f"✅ تم التبديل إلى **التداول الحقيقي**. الرصيد الحقيقي: ${real_balance:.2f} USDT")
                                        save_state()
                                    else:
                                        PAPER_TRADING = True
                                        send_telegram("❌ فشل جلب الرصيد الحقيقي من Binance. تأكد من اتصال API والمفاتيح.\n⚠️ لم يتم التبديل إلى التداول الحقيقي. بقيت في **الوضع الورقي**.")
                                else: send_telegram("ℹ️ أنت بالفعل في وضع التداول الحقيقي.")
                            elif text == 'تداول ورقي':
                                if not ENABLE_TRADING: send_telegram("⚠️ التداول غير مفعل في الإعدادات الأساسية (ENABLE_TRADING=false). لا يمكن التحويل.")
                                elif not PAPER_TRADING and not TEST_MODE:
                                    if open_positions: send_telegram("🔄 جاري إغلاق جميع المراكز الحقيقية قبل التحويل إلى التداول الورقي...")
                                    close_all_positions("التحويل إلى التداول الورقي - إغلاق المراكز الحقيقية")
                                    time.sleep(2)
                                    with _global_state_lock:
                                        open_positions.clear()
                                        _daily_loss_tracker = 0.0; _daily_trades_count = 0; _daily_winning_trades = 0; _daily_losing_trades = 0
                                        _daily_biggest_win = 0.0; _daily_biggest_loss = 0.0; _daily_most_traded.clear()
                                        _daily_total_holding_time_win = 0.0; _daily_total_holding_time_loss = 0.0
                                        _daily_holding_count_win = 0; _daily_holding_count_loss = 0
                                        bot_stats.total_pnl_usdt = 0.0; bot_stats.winning_trades = 0; bot_stats.losing_trades = 0
                                        bot_stats.total_trades = 0; bot_stats.daily_pnl = 0.0; bot_stats.weekly_pnl = 0.0
                                        bot_stats.weekly_wins = 0; bot_stats.weekly_losses = 0
                                        bot_stats.last_balance = PAPER_INITIAL_BALANCE
                                        bot_stats.equity_curve = deque(maxlen=1000)
                                        bot_stats.symbol_performance = {}
                                        _daily_trades_date = datetime.now().date()
                                    PAPER_TRADING = True; TEST_MODE = False
                                    send_telegram(f"✅ تم التبديل إلى **التداول الورقي** برصيد ابتدائي ${PAPER_INITIAL_BALANCE:.2f} USDT.")
                                    save_state()
                                else: send_telegram("ℹ️ أنت بالفعل في وضع التداول الورقي أو المحاكاة.")
            time.sleep(1)
        except Exception as e:
            logger.error(f"خطأ في تلغرام: {e}. سأحاول مجدداً بعد 10 ثوانٍ.")
            generate_error_report("فشل_خيط", "تلغرام", str(e))
            time.sleep(10)

class MarketScanner:
    def __init__(self):
        self.last_scan = 0
        self.candidates = []
        self.scores = {}
    def scan(self):
        global _last_scan_candidates
        tickers = fetch_tickers_with_retry()
        if not tickers: send_telegram("⚠️ فشل جلب بيانات الأسواق أثناء المسح."); return
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
        if not new5: send_telegram("🔍 مسح السوق: لا توجد أي عملات تفي بالشروط حالياً.")
        else:
            if new5 != _last_scan_candidates: msg = "<b>🔍 أفضل 5 فرص ساخنة (جديدة)</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in new5); send_telegram(msg)
            else:
                next_candidates = self.candidates[5:9]
                if next_candidates: msg = "<b>🔄 فرص بديلة للتحليل (القائمة الأولى ثابتة)</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in next_candidates); send_telegram(msg)
                else: msg = "<b>⚠️ نفس القائمة ولا توجد بدائل</b>\n" + "\n".join(f"{sym} ({self.scores[sym]:.1f})" for sym in new5); send_telegram(msg)
        _last_scan_candidates = new5
        self.last_scan = time.time()
    def should_scan(self): return (time.time()-self.last_scan) > SCAN_INTERVAL_MINUTES*60

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
        'sync_preemptive_stop_orders': sync_preemptive_stop_orders,
        'periodic_status_report': periodic_status_report,
        'lock_health_monitor': lock_health_monitor,
        'trades_cache_cleanup': trades_cache_cleanup,
        'memory_watchdog': memory_watchdog,
        'auto_recovery_monitor': auto_recovery_monitor
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

# --------------------------- دوال حفظ واستعادة الحالة ---------------------------
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
                'preemptive_sl_order_id': p.preemptive_sl_order_id,
                'last_trailing_stop_sent': p.last_trailing_stop_sent,
                'last_target_hit_time': p.last_target_hit_time.isoformat() if p.last_target_hit_time else None,
                'last_target_hit_index': p.last_target_hit_index,
                'sold_at_15': p.sold_at_15,
                'sold_at_20': p.sold_at_20
            }
        data = {
            'open_positions':pos_dict,
            'last_close_time':{k:v.isoformat() for k,v in last_close_time.items()},
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
                'manipulation': FILTER_MANIPULATION_ENABLED,
                'liquidity': FILTER_LIQUIDITY_ENABLED,
                'market_cap': FILTER_MARKET_CAP_ENABLED,
                'volume_24h': FILTER_VOLUME_24H_ENABLED,
                'change_24h': FILTER_CHANGE_24H_ENABLED,
                'hour_candle': FILTER_HOUR_CANDLE_ENABLED,
                '4h_candle': FILTER_4H_CANDLE_ENABLED,
                'cooldown_hours': BASE_COOLDOWN_HOURS,
                'buy_committee_multiplier': CURRENT_BUY_COMMITTEE_MULTIPLIER,
                'strength_threshold': STRENGTH_THRESHOLD,
                'scalp_min_profit': SCALP_MIN_PROFIT,
                'manipulation_threshold': MANIPULATION_REJECT_THRESHOLD,
                'sell_threshold': SELL_DECISION_THRESHOLD,
                'stop_loss_25': STOP_LOSS_PARTIAL_1_PERCENT,
                'stop_loss_33': STOP_LOSS_PARTIAL_2_PERCENT,
                'stop_loss_100': STOP_LOSS_FULL_PERCENT
            }
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
    global open_positions, last_close_time, _daily_loss_tracker, _daily_trades_count, _daily_trades_date
    global PAUSED, daily_loss_cooldown_until, bot_stats
    global _daily_winning_trades, _daily_losing_trades, _local_pending_symbols, _exchange_pending_symbols
    global FILTER_MANIPULATION_ENABLED, FILTER_LIQUIDITY_ENABLED, FILTER_MARKET_CAP_ENABLED
    global FILTER_VOLUME_24H_ENABLED, FILTER_CHANGE_24H_ENABLED, FILTER_HOUR_CANDLE_ENABLED, FILTER_4H_CANDLE_ENABLED
    global BASE_COOLDOWN_HOURS, COOLDOWN_HOURS_GOOD, COOLDOWN_HOURS_BAD
    global CURRENT_BUY_COMMITTEE_MULTIPLIER, STRENGTH_THRESHOLD, SCALP_MIN_PROFIT
    global MANIPULATION_REJECT_THRESHOLD, SELL_DECISION_THRESHOLD
    global STOP_LOSS_PARTIAL_1_PERCENT, STOP_LOSS_PARTIAL_2_PERCENT, STOP_LOSS_FULL_PERCENT
    global buying_committee
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
            pos = Position(pdata['symbol'], pdata['side'], pdata['total_size'], pdata['entry_price'], pdata['atr'],
                           pdata['stop_loss'], None, pdata['symbol_type'], pdata['pred'], pdata['confidence'],
                           pdata['regime'], tp_levels)
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
            pos.preemptive_sl_order_id = pdata.get('preemptive_sl_order_id')
            pos.last_trailing_stop_sent = pdata.get('last_trailing_stop_sent')
            pos.last_target_hit_time = datetime.fromisoformat(pdata['last_target_hit_time']) if pdata.get('last_target_hit_time') else None
            pos.last_target_hit_index = pdata.get('last_target_hit_index', -1)
            pos.sold_at_15 = pdata.get('sold_at_15', False)
            pos.sold_at_20 = pdata.get('sold_at_20', False)
            open_positions[sym] = pos
        removed = []
        for sym in list(open_positions.keys()):
            if not validate_restored_position(sym, open_positions[sym]):
                removed.append(sym)
                del open_positions[sym]
                last_close_time[sym] = datetime.now()
        if removed: send_telegram(f"⚠️ تم اكتشاف {len(removed)} مركزاً أُغلقت أثناء التعطل: {', '.join(removed)}"); save_state()
        state_age = time.time() - os.path.getmtime(STATE_FILE)
        if state_age > 1800:
            _local_pending_symbols.clear()
            _exchange_pending_symbols.clear()
            logger.info("تم مسح الرموز المعلقة لأن الحالة المحفوظة أقدم من 30 دقيقة")
        else:
            _local_pending_symbols = set(data.get('_local_pending_symbols', []))
            _exchange_pending_symbols = set(data.get('_exchange_pending_symbols', []))
        last_close_time = {k: datetime.fromisoformat(v) for k,v in data.get('last_close_time',{}).items()}
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
        FILTER_MANIPULATION_ENABLED = filters.get('manipulation', True)
        FILTER_LIQUIDITY_ENABLED = filters.get('liquidity', True)
        FILTER_MARKET_CAP_ENABLED = filters.get('market_cap', True)
        FILTER_VOLUME_24H_ENABLED = filters.get('volume_24h', True)
        FILTER_CHANGE_24H_ENABLED = filters.get('change_24h', True)
        FILTER_HOUR_CANDLE_ENABLED = filters.get('hour_candle', True)
        FILTER_4H_CANDLE_ENABLED = filters.get('4h_candle', True)
        new_cooldown = filters.get('cooldown_hours', BASE_COOLDOWN_HOURS)
        BASE_COOLDOWN_HOURS = new_cooldown
        COOLDOWN_HOURS_GOOD = new_cooldown * 0.5
        COOLDOWN_HOURS_BAD = new_cooldown * 1.5
        CURRENT_BUY_COMMITTEE_MULTIPLIER = filters.get('buy_committee_multiplier', 1.0)
        STRENGTH_THRESHOLD = filters.get('strength_threshold', 0.25)
        SCALP_MIN_PROFIT = filters.get('scalp_min_profit', 0.20)
        MANIPULATION_REJECT_THRESHOLD = filters.get('manipulation_threshold', 0.65)
        SELL_DECISION_THRESHOLD = filters.get('sell_threshold', 0.65)
        STOP_LOSS_PARTIAL_1_PERCENT = filters.get('stop_loss_25', 0.008)
        STOP_LOSS_PARTIAL_2_PERCENT = filters.get('stop_loss_33', 0.012)
        STOP_LOSS_FULL_PERCENT = filters.get('stop_loss_100', 0.017)
        if 'buying_committee' in globals(): buying_committee.apply_multiplier(CURRENT_BUY_COMMITTEE_MULTIPLIER)
        logger.info(f"تم استعادة {len(open_positions)} مركزاً")
        try:
            sync_preemptive_stop_orders_once()
        except Exception as e:
            logger.warning(f"فشل المزامنة الأولية لأوامر الوقف: {e}")
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
    logger.info("⏳ بدء التشغيل: انتظار 120 ثانية لإعطاء فرصة لرفع الحظر إن وجد...")
    send_telegram("⏳ بدء التشغيل: انتظار دقيقتين قبل الاتصال بـ Binance...")
    time.sleep(120)
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY: logger.critical("❌ مفاتيح API مفقودة"); sys.exit(1)
    scanner = MarketScanner()
    buying_committee = BuyingCommittee()
    load_state()
    if not load_markets_with_retry(max_retries=8, initial_delay=30):
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
            with _global_state_lock: bot_stats.last_balance = real_balance
            logger.info(f"✅ الرصيد الحقيقي: {real_balance:.2f} USDT")
            send_telegram(f"✅ بدء التشغيل في الوضع الحقيقي. الرصيد: ${real_balance:.2f} USDT")
        else:
            logger.error("❌ فشل جلب الرصيد الحقيقي بعد 120 ثانية. سيتم التحول إلى الوضع الورقي.")
            send_telegram("❌ فشل جلب الرصيد الحقيقي. سيتم بدء البوت في **الوضع الورقي** حفاظاً على الأمان.\nيمكنك التبديل لاحقاً باستخدام الأمر `تداول حقيقي` بعد التحقق من الإعدادات.")
            PAPER_TRADING = True; TEST_MODE = False
            with _global_state_lock: bot_stats.last_balance = PAPER_INITIAL_BALANCE
            save_state()
    mode_str = "محاكاة" if TEST_MODE else ("ورقي" if PAPER_TRADING else ("Testnet" if BINANCE_SANDBOX else "حقيقي"))
    logger.info(f"🚀 بدء البوت v27.7 - الوضع: {mode_str}")
    send_telegram(f"🚀 بوت v27.7 يعمل – الوضع: {mode_str}\n✅ تحكم كامل بالفلاتر والعتبات عبر تلغرام\n✅ أمر 'رصيدي' لعرض الرصيد الحر + قيمة المراكز\n✅ تقرير يومي يعرض صافي الربح/الخسارة\n✅ التحويل بين التداول الورقي والحقيقي: `تداول حقيقي` / `تداول ورقي`\n✅ تحسينات كبيرة في الذاكرة ومنع التسريب\n✅ WebSocket خالص مع إعادة تشغيل تلقائي\n✅ مراقبة وإعادة تشغيل جميع الخيوط بما فيها WebSocket (مع حماية watchdog)\n✅ جلب صبور للرصيد الحقيقي (حتى 120 ثانية)\n✅ سرعة مراقبة المراكز 3 ثوانٍ\n✅ التحكم بدرجة الاشتباه: `رفع عتبة الاشتباه 70%` أو `خفض عتبة الاشتباه 50%`\n✅ التحكم بعتبة البيع: `رفع عتبة البيع 70%` أو `خفض عتبة البيع 55%`\n✅ التحكم بعتبات الخسارة الوقائية:\n   - `تعيين عتبة خسارة 25% 0.9`\n   - `تعيين عتبة خسارة 33% 1.5`\n   - `تعيين عتبة خسارة 100% 2.0`\n   - `عتبات الخسارة` لعرض القيم الحالية\n✅ البيع التدريجي الصارم (بدون إغلاق مبكر بسبب المبلغ الصغير)\n✅ نسب البيع: 15%, 15%, 15%, 15%, 20%, 50%, 100%\n✅ دعم أفضل لمنصة Render (منفذ 8080)\n✅ **إصلاح ذاتي كامل - لا حاجة لإعادة تشغيل خارجية**\n✅ **الوقف المسبق الموحد للعادي والميم: تفعيل عند 1.7% وتنفيذ مباشر عند 2.1%**\n✅ **إضافة خيط auto_recovery لإعادة ضبط الاتصال عند حظر Binance**")
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