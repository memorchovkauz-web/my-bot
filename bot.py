import os
import io
import zipfile
from xml.sax.saxutils import escape
import json
import re
import time
import logging
import traceback
import threading
import asyncio
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import contextvars
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from telegram.error import BadRequest

TOKEN = os.getenv("BOT_TOKEN")

# ================= V20 ERROR LOGGING =================
# Production log: Render Logs'да хатони аниқ кўриш учун.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("autobaza-bot")


# ================= MAINTENANCE / DEPLOY MODE =================
# Deploy пайтида user'ларга тушунарли хабар бериш учун.
# ENV: MAINTENANCE_MODE=true бўлса, бот асосий flow'ларни тўхтатиб,
# фақат техник янгиланиш хабарини қайтаради.
# Default false — оддий ишлашга таъсир қилмайди.
MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "false").strip().lower() in ("1", "true", "yes", "on")

MAINTENANCE_TEXT = (
    "⚙️ Ботда техник янгиланиш кетмоқда.\n\n"
    "Илтимос, 1 дақиқадан кейин қайта уриниб кўринг."
)


async def maintenance_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Deploy mode ёқилганда callback/message/photo/video/contact flow'ларни хавфсиз тўхтатади."""
    if not MAINTENANCE_MODE:
        return False

    try:
        if update.callback_query:
            try:
                await update.callback_query.answer("⚙️ Техник янгиланиш кетмоқда", show_alert=False)
            except Exception:
                pass

            if update.callback_query.message:
                await update.callback_query.message.reply_text(MAINTENANCE_TEXT)
            return True

        if update.effective_message:
            await update.effective_message.reply_text(MAINTENANCE_TEXT)
            return True

    except Exception:
        logger.exception("MAINTENANCE GUARD ERROR")
        return True

    return True


# ================= KEEP ALIVE + WEBHOOK SERVER v10 =================
# Render Web Service free plan portни тез кўриши учун HTTP server
# DB/Google Sheets оғир инициализациясидан ОЛДИН старт бўлади.
# Бу блок bot flow, callback, DB status ва меню структурасига тегмайди.
MAIN_LOOP = None


def log_webhook_future_exception(future):
    """Webhook thread ичида process_update exception бўлса, ботни йиқитмасдан log қилади."""
    try:
        future.result()
    except Exception:
        logger.exception("WEBHOOK PROCESS_UPDATE ERROR")

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # UptimeRobot health check log'ларини кўпайтирмаслик учун.
        return

    def do_GET(self):
        if self.path == "/" or self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is running")
            return

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
        return

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        return

    def do_POST(self):
        # App ҳали тайёр бўлмаса, Telegram қайта-қайта юбориб ботни оғирлаштирмаслиги учун OK қайтарамиз.
        if MAIN_LOOP is None or "app" not in globals() or not TOKEN:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        expected_path = f"/{TOKEN}"
        if self.path != expected_path:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length)
            update_data = json.loads(raw_body.decode("utf-8"))
            telegram_update = Update.de_json(update_data, app.bot)

            future = asyncio.run_coroutine_threadsafe(
                app.process_update(telegram_update),
                MAIN_LOOP
            )
            future.add_done_callback(log_webhook_future_exception)

            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return
        except Exception as e:
            logger.exception("WEBHOOK POST ERROR")
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

def run_server():
    port = int(os.environ.get("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    # ThreadingHTTPServer: Telegram webhook + UptimeRobot health checks бир-бирини кутиб қолмасин.
    print(f"KEEP ALIVE SERVER STARTED ON PORT {port}")
    server.serve_forever()

# Render портни DB/Sheetsдан олдин кўриши учун server дарҳол старт олади.
threading.Thread(target=run_server, daemon=True).start()

DATABASE_URL = os.getenv("DATABASE_URL")

TASHKENT_TZ = ZoneInfo("Asia/Tashkent")

# ================= DB POOL v17 =================
# Мақсад: 70-80 user бир вақтда ишлатганда битта global cursor bottleneck бўлмасин.
# Эски код структураси сақланади: cursor.execute(...), cursor.fetchone(), conn.commit() ишлашда давом этади.
# Ичкарида ҳар asyncio task учун pool'дан алоҳида connection/cursor берилади.
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "8"))

_db_pool = None
_db_conn_var = contextvars.ContextVar("db_conn", default=None)
_db_cursor_var = contextvars.ContextVar("db_cursor", default=None)
_db_readonly_var = contextvars.ContextVar("db_readonly", default=False)

def create_db_pool():
    global _db_pool
    if _db_pool is None:
        _db_pool = ThreadedConnectionPool(
            DB_POOL_MIN,
            DB_POOL_MAX,
            DATABASE_URL,
            options="-c timezone=Asia/Tashkent"
        )
    return _db_pool

def reset_db_pool():
    global _db_pool
    try:
        old_pool = _db_pool
        _db_pool = None
        if old_pool is not None:
            old_pool.closeall()
    except Exception as e:
        print("DB POOL RESET ERROR:", e)
    finally:
        _db_conn_var.set(None)
        _db_cursor_var.set(None)
        _db_readonly_var.set(False)
        create_db_pool()

def _release_db_connection(commit=False, rollback=False):
    pool = create_db_pool()
    cur = _db_cursor_var.get()
    db_conn = _db_conn_var.get()

    if cur is not None:
        try:
            cur.close()
        except Exception:
            pass

    if db_conn is not None:
        try:
            if commit:
                db_conn.commit()
            elif rollback:
                db_conn.rollback()
        except Exception as e:
            print("DB COMMIT/ROLLBACK ERROR:", e)
            try:
                db_conn.rollback()
            except Exception:
                pass
        finally:
            try:
                pool.putconn(db_conn)
            except Exception:
                pass

    _db_conn_var.set(None)
    _db_cursor_var.set(None)
    _db_readonly_var.set(False)

def _is_readonly_query(query):
    q = str(query or "").strip().lower()
    return q.startswith(("select", "with", "show"))

def _get_task_cursor(query=None):
    cur = _db_cursor_var.get()
    if cur is not None:
        return cur

    pool = create_db_pool()
    db_conn = pool.getconn()
    cur = db_conn.cursor()
    _db_conn_var.set(db_conn)
    _db_cursor_var.set(cur)
    _db_readonly_var.set(_is_readonly_query(query))
    return cur

class PooledCursorProxy:
    def execute(self, query, params=None):
        cur = _get_task_cursor(query)
        return cur.execute(query, params or ())

    def fetchone(self):
        cur = _db_cursor_var.get()
        if cur is None:
            return None
        row = cur.fetchone()
        if _db_readonly_var.get():
            _release_db_connection()
        return row

    def fetchall(self):
        cur = _db_cursor_var.get()
        if cur is None:
            return []
        rows = cur.fetchall()
        if _db_readonly_var.get():
            _release_db_connection()
        return rows

    def fetchmany(self, size=None):
        cur = _db_cursor_var.get()
        if cur is None:
            return []
        rows = cur.fetchmany(size) if size is not None else cur.fetchmany()
        if _db_readonly_var.get():
            _release_db_connection()
        return rows

    @property
    def rowcount(self):
        cur = _db_cursor_var.get()
        return cur.rowcount if cur is not None else -1

    @property
    def description(self):
        cur = _db_cursor_var.get()
        return cur.description if cur is not None else None

class PooledConnectionProxy:
    def cursor(self):
        return PooledCursorProxy()

    def commit(self):
        _release_db_connection(commit=True)

    def rollback(self):
        _release_db_connection(rollback=True)

# Эски код учун global nomlar сақланади.
create_db_pool()
conn = PooledConnectionProxy()
cursor = conn.cursor()
try:
    cursor.execute("SET TIME ZONE 'Asia/Tashkent'")
    conn.commit()
except Exception as e:
    print("DB TIMEZONE SET ERROR:", e)
    conn.rollback()

# =====================
# SAFE SPEED CACHE v9
# =====================
# Қисқа TTL cache: менюларда такрорий SELECT'ларни камайтиради.
# DB структураси ва callback flow ўзгармайди.
CACHE_TTL_SECONDS = 45
_speed_cache = {}

def cache_get(key):
    try:
        item = _speed_cache.get(key)
        if not item:
            return None
        created_at, value = item
        if time.time() - created_at > CACHE_TTL_SECONDS:
            _speed_cache.pop(key, None)
            return None
        return value
    except Exception:
        return None

def cache_set(key, value):
    try:
        _speed_cache[key] = (time.time(), value)
    except Exception:
        pass
    return value

def clear_speed_cache(prefix=None):
    try:
        if prefix is None:
            _speed_cache.clear()
            return
        for key in list(_speed_cache.keys()):
            if str(key).startswith(str(prefix)):
                _speed_cache.pop(key, None)
    except Exception:
        pass

def clear_driver_cache():
    clear_speed_cache("driver:")
    clear_speed_cache("driver_by_car:")

def clear_car_cache():
    clear_speed_cache("cars:")
    clear_speed_cache("car_type_by_number:")


cursor.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE,
    name TEXT,
    surname TEXT,
    phone TEXT,
    firm TEXT,
    car TEXT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW()
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cars (
    id SERIAL PRIMARY KEY,
    firm TEXT,
    car_number TEXT UNIQUE,
    car_type TEXT,
    status TEXT,
    fuel_type TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS fuel_reports (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT,
    car TEXT,
    fuel_type TEXT,
    km TEXT,
    video_id TEXT,
    photo_id TEXT,
    created_at TIMESTAMP DEFAULT NOW()
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS gas_transfers (
    id BIGSERIAL PRIMARY KEY,
    from_driver_id BIGINT,
    from_car TEXT,
    to_driver_id BIGINT,
    to_car TEXT,
    firm TEXT,
    note TEXT,
    video_id TEXT,
    status TEXT DEFAULT 'Текширувда',
    receiver_comment TEXT,
    approved_by_id BIGINT,
    approved_by_name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    answered_at TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS diesel_transfers (
    id BIGSERIAL PRIMARY KEY,
    from_driver_id BIGINT,
    from_car TEXT,
    to_driver_id BIGINT,
    to_car TEXT,
    firm TEXT,
    liter TEXT,
    note TEXT,
    speedometer_photo_id TEXT,
    video_id TEXT,
    status TEXT DEFAULT 'Қабул қилувчи текширувида',
    receiver_comment TEXT,
    approved_by_id BIGINT,
    approved_by_name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    answered_at TIMESTAMP
)
""")


try:
    cursor.execute("ALTER TABLE diesel_transfers ADD COLUMN IF NOT EXISTS speedometer_photo_id TEXT")
    conn.commit()
except Exception as e:
    print("DIESEL TRANSFERS ADD speedometer_photo_id ERROR:", e)
    conn.rollback()

# V24: Excel ҳисоботда “ким тасдиқлаган” чиқиши учун хавфсиз колонкалар.
# Эски маълумотларга таъсир қилмайди, фақат кейинги тасдиқларда ID сақланади.
try:
    cursor.execute("ALTER TABLE diesel_transfers ADD COLUMN IF NOT EXISTS approved_by_id BIGINT")
    cursor.execute("ALTER TABLE diesel_transfers ADD COLUMN IF NOT EXISTS approved_by_name TEXT")
    cursor.execute("ALTER TABLE diesel_prihod ADD COLUMN IF NOT EXISTS approved_by_id BIGINT")
    cursor.execute("ALTER TABLE diesel_prihod ADD COLUMN IF NOT EXISTS approved_by_name TEXT")
    conn.commit()
except Exception as e:
    print("DIESEL REPORT approved_by_id ALTER ERROR:", e)
    conn.rollback()


cursor.execute("""
CREATE TABLE IF NOT EXISTS diesel_prihod (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT,
    liter NUMERIC,
    note TEXT,
    video_id TEXT,
    photo_id TEXT,
    status TEXT DEFAULT 'Текширувда',
    receiver_comment TEXT,
    approved_by_id BIGINT,
    approved_by_name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    answered_at TIMESTAMP
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS diesel_other_expense (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT,
    liter TEXT,
    note TEXT,
    video_id TEXT,
    status TEXT DEFAULT 'Тасдиқланди',
    created_at TIMESTAMP DEFAULT NOW()
)
""")


conn.commit()

USERS = {
    492894595: {"role": "director", "name": "Jahongir Ganiyev"},
    492894594: {"role": "technadzor", "name": "Jahongir Ganiyev"},
    1973869412: {"role": "technadzor", "name": "офис"},
    444444444: {"role": "slesar", "name": "Слесарь исми"},
}

SHEET_NAME = "Avtobaza Remont Baza"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
client = gspread.authorize(creds)


def gspread_retry(action_name, func, attempts=6, base_delay=3):
    """Google Sheets баъзида 500/Internal error беради.
    Шу ҳолатда бот дарров ўчмаслиги учун қайта уриниб кўрамиз.
    """
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            last_error = e
            wait_seconds = base_delay * attempt
            print(f"[GSPREAD RETRY] {action_name} failed ({attempt}/{attempts}): {e}")
            if attempt < attempts:
                time.sleep(wait_seconds)

    print(f"[GSPREAD ERROR] {action_name} failed after {attempts} attempts: {last_error}")
    raise last_error


sheet = gspread_retry("open spreadsheet", lambda: client.open(SHEET_NAME))

remont_ws = gspread_retry("open worksheet REMONT", lambda: sheet.worksheet("REMONT"))

def sync_repairs_to_db():
    rows = remont_ws.get_all_values()[1:]

    for row in rows:
        try:
            car_number = row[2].strip() if len(row) > 2 else ""
            km = row[3].strip() if len(row) > 3 else ""
            repair_type = row[4].strip() if len(row) > 4 else ""
            status = row[5].strip() if len(row) > 5 else ""
            comment = row[6].strip() if len(row) > 6 else ""
            video_id = row[7].strip() if len(row) > 7 else ""
            photo_id = row[8].strip() if len(row) > 8 else ""
            person = row[9].strip() if len(row) > 9 else ""
            entered_at = row[10].strip() if len(row) > 10 else ""
            exited_at = row[11].strip() if len(row) > 11 else ""

            if not car_number:
                continue

            cursor.execute("""
                INSERT INTO repairs (
                    car_number,
                    km,
                    repair_type,
                    status,
                    comment,
                    enter_video,
                    enter_photo,
                    entered_by,
                    exited_by,
                    entered_at,
                    exited_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULLIF(%s, '')::timestamp, NULLIF(%s, '')::timestamp)
            """, (
                car_number,
                km,
                repair_type,
                status,
                comment,
                video_id,
                photo_id,
                person,
                person,
                entered_at,
                exited_at
            ))

        except Exception as e:
            print("REPAIR SYNC ERROR:", e)

    conn.commit()


# sync_repairs_to_db()
# print("REPAIRS SYNCED")

def save_new_repair_to_db(
    car_number,
    km,
    repair_type,
    status,
    comment,
    video_id,
    photo_id,
    person,
    entered_at,
    exited_at
):
    cursor.execute("""
        INSERT INTO repairs (
            car_number,
            km,
            repair_type,
            status,
            comment,
            enter_video,
            enter_photo,
            entered_by,
            exited_by,
            entered_at,
            exited_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULLIF(%s, '')::timestamp, NULLIF(%s, '')::timestamp)
    """, (
        car_number,
        km,
        repair_type,
        status,
        comment,
        video_id,
        photo_id,
        person,
        person,
        entered_at,
        exited_at
    ))

    conn.commit()

mashina_ws = gspread_retry("open worksheet MASHINALAR", lambda: sheet.worksheet("MASHINALAR"))

def sync_cars_to_db():
    cars = mashina_ws.get_all_values()[1:]

    for row in cars:
        try:
            firm = row[0].strip() if len(row) > 0 else ""
            car_number = row[1].strip() if len(row) > 1 else ""
            car_type = row[2].strip() if len(row) > 2 else ""
            status = row[6].strip() if len(row) > 6 else ""
            fuel_type = row[7].strip() if len(row) > 7 else ""

            if not car_number:
                continue

            cursor.execute("""
                INSERT INTO cars (
                    firm,
                    car_number,
                    car_type,
                    fuel_type,
                    status
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (car_number)
                DO UPDATE SET
                    firm = EXCLUDED.firm,
                    car_type = EXCLUDED.car_type,
                    fuel_type = EXCLUDED.fuel_type,
                    status = EXCLUDED.status
            """, (
                firm,
                car_number,
                car_type,
                fuel_type,
                status
            ))

        except Exception as e:
            print("CAR SYNC ERROR:", e)

    conn.commit()

sync_cars_to_db()
clear_car_cache()

print("CARS SYNCED")

drivers_ws = gspread_retry("open worksheet DRIVERS", lambda: sheet.worksheet("DRIVERS"))

def sync_drivers_to_db():
    drivers = drivers_ws.get_all_values()[1:]

    for row in drivers:
        try:
            telegram_id = row[0].strip() if len(row) > 0 else ""
            name = row[1].strip() if len(row) > 1 else ""
            surname = row[2].strip() if len(row) > 2 else ""
            phone = row[3].strip() if len(row) > 3 else ""
            firm = row[4].strip() if len(row) > 4 else ""
            car = row[5].strip() if len(row) > 5 else ""
            status = row[6].strip() if len(row) > 6 else ""
            work_role = row[8].strip() if len(row) > 8 and row[8].strip() else "driver"

            if not telegram_id:
                continue

            cursor.execute("""
                INSERT INTO drivers (
                    telegram_id,
                    name,
                    surname,
                    phone,
                    firm,
                    car,
                    status,
                    work_role
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (telegram_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    surname = EXCLUDED.surname,
                    phone = EXCLUDED.phone,
                    firm = EXCLUDED.firm,
                    car = EXCLUDED.car,
                    work_role = CASE
                        WHEN COALESCE(drivers.work_role, '') <> '' THEN drivers.work_role
                        ELSE EXCLUDED.work_role
                    END,
                    status = CASE
                        WHEN TRIM(COALESCE(drivers.status, '')) IN ('Тасдиқланди', 'Рад этилди')
                             AND TRIM(COALESCE(EXCLUDED.status, '')) = 'Текширувда'
                        THEN drivers.status
                        ELSE EXCLUDED.status
                    END
            """, (
                int(telegram_id),
                name,
                surname,
                phone,
                firm,
                car,
                status,
                work_role
            ))

        except Exception as e:
            print("DRIVER SYNC ERROR:", e)

    conn.commit()


sync_drivers_to_db()
print("DRIVERS SYNCED")

FIRM_NAMES = [
    "Мемор Уткир Човка",
    "Сам Техно Строй Инвест",
    "Меьмар",
    "Якдона",
]

REPAIR_TYPES = [
    "Мой алмаштириш",
    "Ходовой ремонт",
    "Мотор ремонт",
    "Кузов ремонт",
    "Диагностика",
    "Балон ремонт",
    "Бошқалар",
]

STATUS_ORDER = {
    "текширувда": 0,
    "соз": 1,
    "носоз": 2,
}


def now_text():
    return datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def is_valid_km(value):
    return value.isdigit() and 1 <= len(value) <= 8


def is_valid_note(value):
    return bool(value and value.strip()) and len(value.strip()) >= 2

def is_valid_name(value):
    value = (value or "").strip()
    return value.isalpha() and len(value) >= 2


def is_valid_phone_number(value):
    phone = (value or "").replace(" ", "").replace("+", "").replace("-", "").strip()
    return phone.isdigit() and len(phone) == 12 and phone.startswith("998")


def clean_phone_number(value):
    return (value or "").replace(" ", "").replace("+", "").replace("-", "").strip()


def calculate_duration(start_time, end_time):
    try:
        start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        diff = end - start
        total_minutes = int(diff.total_seconds() // 60)
        return f"{total_minutes // 60} соат {total_minutes % 60} дақиқа"
    except Exception:
        return ""


def get_period_dates(period):
    now = datetime.now(TASHKENT_TZ)

    if period == "10":
        return now - timedelta(days=10), now
    if period == "30":
        return now - timedelta(days=30), now
    if period == "this_month":
        return now.replace(day=1, hour=0, minute=0, second=0), now
    if period == "last_month":
        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0)
        last_month_end = first_day_this_month - timedelta(seconds=1)
        start = last_month_end.replace(day=1, hour=0, minute=0, second=0)
        return start, last_month_end
    if period == "year":
        return now - timedelta(days=365), now

    return None, None


def get_user(update):
    return USERS.get(update.effective_user.id)


def get_role(update):
    user = get_user(update)
    if user:
        return user["role"]

    try:
        user_id = int(update.effective_user.id)
        cache_key = f"driver:role:{user_id}"
        cached = cache_get(cache_key)
        if cached is not None:
            return cached or None

        cursor.execute("""
            SELECT work_role, status
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (user_id,))
        row = cursor.fetchone()

        if not row:
            return cache_set(cache_key, "") or None

        work_role = row[0] or "driver"
        status = (row[1] or "").strip()

        if status != "Тасдиқланди":
            return cache_set(cache_key, "") or None

        if work_role in ["driver", "mechanic", "zapravshik"]:
            return cache_set(cache_key, work_role)

        return cache_set(cache_key, "") or None

    except Exception as e:
        print("GET ROLE FROM DB ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None

def get_user_name(update):
    user = get_user(update)
    return user["name"] if user else "Номаълум"

def get_driver_status(user_id):
    global conn, cursor

    cache_key = f"driver:status:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT status
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(user_id),))

        row = cursor.fetchone()

        if row:
            return cache_set(cache_key, row[0] or "")

        return cache_set(cache_key, None)

    except Exception as e:
        print("GET DRIVER STATUS ERROR:", e)

        try:
            conn.rollback()
        except:
            pass

        try:
            reset_db_pool()

            cursor.execute("""
                SELECT status
                FROM drivers
                WHERE telegram_id = %s
                LIMIT 1
            """, (int(user_id),))

            row = cursor.fetchone()

            if row:
                return cache_set(cache_key, row[0] or "")

        except Exception as e2:
            print("RECONNECT DRIVER STATUS ERROR:", e2)

    return None

def get_driver_car(user_id):
    cache_key = f"driver:car:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT car
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()
    value = (row[0] or "") if row else ""
    return cache_set(cache_key, value)


def get_driver_work_role(user_id):
    cache_key = f"driver:work_role:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT work_role
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()
    value = (row[0] or "driver") if row else "driver"
    return cache_set(cache_key, value)


def get_driver_full_name_by_telegram_id(user_id):
    cache_key = f"driver:full_name:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT name, surname
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(user_id),))

        row = cursor.fetchone()
        if not row:
            return cache_set(cache_key, "")

        name = (row[0] or "").strip()
        surname = (row[1] or "").strip()
        full_name = f"{surname} {name}".strip()
        return cache_set(cache_key, full_name)

    except Exception as e:
        print("GET DRIVER FULL NAME ERROR:", e)
        try:
            conn.rollback()
        except:
            pass
        return ""


def diesel_sender_display_name(from_car, from_driver_id=None, context=None):
    # Агар дизелни заправщик берган бўлса, карточкада "Заправщик" эмас,
    # заправщикнинг Ф.И.Ш. кўринсин. DB status ва from_car структураси ўзгармайди.
    if str(from_car or "").strip() == "Заправщик":
        giver_name = ""

        if context is not None:
            giver_name = (context.user_data.get("dieselgive_from_name") or "").strip()

        if not giver_name and from_driver_id:
            giver_name = get_driver_full_name_by_telegram_id(from_driver_id)

        return giver_name or "Заправщик"

    return str(from_car or "-")


def diesel_sender_line(from_car, from_driver_id=None, context=None):
    # Заправщик берган дизелда "Заправщик" техника номи эмас, Ф.И.Ш. кўринади.
    # DBдаги from_car структураси ўзгармайди.
    from_display = diesel_sender_display_name(from_car, from_driver_id=from_driver_id, context=context)
    if str(from_car or "").strip() == "Заправщик":
        return f"🚛 Дизел берган: {from_display}"
    return f"🚛 Дизел берган техника номери: {from_display}"


def work_role_title(work_role):
    titles = {
        "driver": "Ҳайдовчи",
        "mechanic": "Механик",
        "zapravshik": "Заправщик",
    }

    return titles.get(work_role or "driver", work_role or "Ҳайдовчи")


def get_driver_firm(user_id):
    cache_key = f"driver:firm:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT firm
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()
    value = (row[0] or "") if row else ""
    return cache_set(cache_key, value)

def update_driver_status(user_id, status):
    cursor.execute("""
        UPDATE drivers
        SET status = %s
        WHERE telegram_id = %s
    """, (status, int(user_id)))

    conn.commit()
    clear_driver_cache()
    return True

    return False


def get_user_ids_by_role(role_name):
    return [
        user_id for user_id, data in USERS.items()
        if data["role"] == role_name
    ]


async def deny(update):
    await update.effective_message.reply_text("❌ Сизга рухсат йўқ")


def firm_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(name)] for name in FIRM_NAMES],
        resize_keyboard=True
    )


def firm_back_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(name)] for name in FIRM_NAMES] + [[KeyboardButton("⬅️ Орқага")]],
        resize_keyboard=True
    )


def action_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🔧 Ремонтга қўшиш")],
        [KeyboardButton("✅ Ремонтдан чиқариш")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_keyboard():
    total_notifications = pending_registration_count() + pending_repair_exit_count() + pending_diesel_prihod_count()
    notification_text = f"🔔 Уведомления [ {total_notifications} ]" if total_notifications > 0 else "🔔 Уведомления"

    return ReplyKeyboardMarkup([
        [KeyboardButton(notification_text)],
        [KeyboardButton("🔧 Ремонтга қўшиш")],
        [KeyboardButton("👥 Ходимлар")],
        [KeyboardButton("💾 История")],
        [KeyboardButton("📊 Ҳисоботлар")],
    ], resize_keyboard=True)


def technadzor_notifications_keyboard():
    repair_count = pending_repair_exit_count()
    registration_count = pending_registration_count()
    diesel_count = pending_diesel_prihod_count()

    rows = [
        [KeyboardButton(f"📝 Регистрация [ {registration_count} ]")],
        [KeyboardButton(f"🛠 Ремонтдан чиқариш [ {repair_count} ]")],
        [KeyboardButton(f"⛽ Дизел приход [ {diesel_count} ]")],
        [KeyboardButton("⬅️ Орқага")],
    ]

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def technadzor_history_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⛽ История ГАЗ")],
        [KeyboardButton("🟡 История Дизел")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)

def technadzor_reports_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📋 Отчет Ремонт")],
        [KeyboardButton("⛽ Отчет Дизел")],
        [KeyboardButton("🟢 Отчет Газ")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_remont_report_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📄 EXCEL файл")],
        [KeyboardButton("📚 История Ремонт")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_diesel_report_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📄 EXCEL файл")],
        [KeyboardButton("📚 История Дизел")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


# ================= V50: TECHNADZOR DIESEL HISTORY REPORT =================
def technadzor_diesel_history_role_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⛽ Заправщик")],
        [KeyboardButton("🚛 Ҳайдовчи")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_diesel_history_period_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📅 1 кунлик")],
        [KeyboardButton("📅 10 кунлик")],
        [KeyboardButton("📅 1 ойлик")],
        [KeyboardButton("📆 Санадан-санагача")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_main_menu_only_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🏠 Бош меню")],
    ], resize_keyboard=True)


def get_technadzor_diesel_history_dates(period_key):
    now = datetime.now(TASHKENT_TZ)
    if period_key == "1d":
        return now - timedelta(days=1), now
    if period_key == "10d":
        return now - timedelta(days=10), now
    if period_key == "30d":
        return now - timedelta(days=30), now
    return None, None


def parse_technadzor_custom_period(text):
    try:
        raw = (text or "").replace(" ", "")
        if "-" not in raw:
            return None, None
        left, right = raw.split("-", 1)
        start = datetime.strptime(left, "%d.%m.%Y").replace(tzinfo=TASHKENT_TZ, hour=0, minute=0, second=0)
        end = datetime.strptime(right, "%d.%m.%Y").replace(tzinfo=TASHKENT_TZ, hour=23, minute=59, second=59)
        return start, end
    except Exception:
        return None, None


def get_technadzor_zapravshik_diesel_history_rows(start_date, end_date):
    """Returns rows: (kind, id, created_at, car, firm, liter, status, has_media)."""
    rows = []
    try:
        cursor.execute("""
            SELECT
                dp.id,
                dp.created_at,
                dp.liter,
                dp.note,
                dp.status,
                dp.video_id,
                dp.photo_id
            FROM diesel_prihod dp
            LEFT JOIN drivers d ON d.telegram_id = dp.telegram_id
            WHERE COALESCE(d.work_role, '') = 'zapravshik'
              AND dp.created_at BETWEEN %s AND %s
            ORDER BY dp.created_at DESC, dp.id DESC
        """, (start_date, end_date))
        for record_id, created_at, liter, note, status, video_id, photo_id in cursor.fetchall():
            firm, _, _ = parse_diesel_prihod_note(note or "")
            rows.append(("prihod", record_id, created_at, "Заправщик", firm or "-", liter or 0, status or "", bool(video_id or photo_id)))
    except Exception as e:
        print("TECHNADZOR ZAPRAVSHIK DIESEL HISTORY PRIHOD ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass

    try:
        cursor.execute("""
            SELECT
                dt.id,
                dt.created_at,
                dt.to_car,
                dt.firm,
                dt.liter,
                dt.status,
                dt.video_id
            FROM diesel_transfers dt
            WHERE TRIM(COALESCE(dt.from_car, '')) = 'Заправщик'
              AND dt.created_at BETWEEN %s AND %s
            ORDER BY dt.created_at DESC, dt.id DESC
        """, (start_date, end_date))
        for record_id, created_at, to_car, firm, liter, status, video_id in cursor.fetchall():
            rows.append(("rashod", record_id, created_at, to_car or "-", firm or "-", liter or 0, status or "", bool(video_id)))
    except Exception as e:
        print("TECHNADZOR ZAPRAVSHIK DIESEL HISTORY RASHOD ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass

    rows.sort(key=lambda item: item[2] or datetime.min.replace(tzinfo=TASHKENT_TZ), reverse=True)
    return rows


def technadzor_zapravshik_diesel_history_list_keyboard(start_date, end_date):
    rows = get_technadzor_zapravshik_diesel_history_rows(start_date, end_date)
    buttons = []
    for kind, record_id, created_at, car, firm, liter, status, has_media in rows[:80]:
        sign = "➕" if kind == "prihod" else "➖"
        liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter or 0)
        title = f"{sign} {car} / {firm} / {liter_text}л"[:60]
        buttons.append([InlineKeyboardButton(title, callback_data=f"tz_diesel_hist_card|{kind}|{record_id}")])

    if not buttons:
        buttons.append([InlineKeyboardButton("❌ Маълумот топилмади", callback_data="none")])

    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data="tz_diesel_hist_back|period")])
    return InlineKeyboardMarkup(buttons)


# ================= V53: TECHNADZOR DRIVER DIESEL HISTORY =================
def get_technadzor_driver_diesel_history_rows(start_date, end_date):
    """Ҳайдовчи берган дизел расходлари. Returns rows: (kind, id, created_at, car, firm, liter, status, has_media)."""
    rows = []
    try:
        cursor.execute("""
            SELECT
                dt.id,
                dt.created_at,
                dt.from_car,
                dt.firm,
                dt.liter,
                dt.status,
                dt.video_id
            FROM diesel_transfers dt
            WHERE TRIM(COALESCE(dt.from_car, '')) <> 'Заправщик'
              AND dt.created_at BETWEEN %s AND %s
            ORDER BY dt.created_at DESC, dt.id DESC
        """, (start_date, end_date))
        for record_id, created_at, from_car, firm, liter, status, video_id in cursor.fetchall():
            rows.append(("rashod", record_id, created_at, from_car or "-", firm or "-", liter or 0, status or "", bool(video_id)))
    except Exception as e:
        print("TECHNADZOR DRIVER DIESEL HISTORY RASHOD ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
    return rows


def get_technadzor_driver_other_diesel_total(start_date, end_date):
    try:
        cursor.execute("""
            SELECT COALESCE(SUM(liter), 0)
            FROM diesel_transfers
            WHERE TRIM(COALESCE(from_car, '')) <> 'Заправщик'
              AND (TRIM(COALESCE(from_car, '')) = '' OR TRIM(COALESCE(from_car, '')) = '-')
              AND created_at BETWEEN %s AND %s
        """, (start_date, end_date))
        row = cursor.fetchone()
        return row[0] or 0
    except Exception as e:
        print("TECHNADZOR DRIVER OTHER DIESEL TOTAL ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


def technadzor_driver_diesel_history_list_keyboard(start_date, end_date):
    rows = get_technadzor_driver_diesel_history_rows(start_date, end_date)
    buttons = []
    for kind, record_id, created_at, car, firm, liter, status, has_media in rows[:80]:
        liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter or 0)
        title = f"{car} / {firm} / {liter_text}л"[:60]
        buttons.append([InlineKeyboardButton(title, callback_data=f"tz_diesel_hist_card|{kind}|{record_id}")])

    other_total = get_technadzor_driver_other_diesel_total(start_date, end_date)
    other_text = format_liter(other_total) if 'format_liter' in globals() else str(other_total or 0)
    buttons.append([InlineKeyboardButton(f"📦 Бошқа дизел расходлар / {other_text}л", callback_data="none")])

    if not rows:
        buttons.insert(0, [InlineKeyboardButton("❌ Маълумот топилмади", callback_data="none")])

    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data="tz_diesel_hist_back|driver_period")])
    return InlineKeyboardMarkup(buttons)


def technadzor_diesel_history_view_keyboard(kind, record_id, has_media=True):
    buttons = []
    if has_media:
        buttons.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"tz_diesel_hist_view|{kind}|{record_id}")])
    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data="tz_diesel_hist_back|list")])
    return InlineKeyboardMarkup(buttons)


def get_technadzor_diesel_history_card(kind, record_id, context=None):
    if kind == "prihod":
        if context is None:
            return "❌ Маълумот топилмади.", False
        row = diesel_prihod_row_to_context(record_id, context)
        if not row:
            return "❌ Маълумот топилмади.", False
        status = context.user_data.get("diesel_prihod_status") or "-"
        text = diesel_prihod_card_text(context, status=status)
        has_media = bool(context.user_data.get("diesel_prihod_video_id") or context.user_data.get("diesel_prihod_photo_id"))
        return text, has_media

    row = get_diesel_transfer_full_row(record_id)
    if not row:
        return "❌ Маълумот топилмади.", False
    text = diesel_transfer_sender_card_text(row, status_text=diesel_status_display(row[9]))
    has_media = bool(row[8])
    return text, has_media


async def send_technadzor_diesel_history_media(query, kind, record_id, context):
    if kind == "prihod":
        row = diesel_prihod_row_to_context(record_id, context)
        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return
        photo_id = context.user_data.get("diesel_prihod_photo_id")
        video_id = context.user_data.get("diesel_prihod_video_id")
        if photo_id:
            try:
                await query.message.chat.send_photo(photo_id, caption="🖼 Дизел приход расми")
            except Exception:
                pass
        if video_id:
            try:
                await query.message.chat.send_video_note(video_id)
            except Exception:
                try:
                    await query.message.chat.send_video(video_id, caption="🎥 Дизел приход видео")
                except Exception:
                    pass
        await query.message.chat.send_message(
            diesel_prihod_card_text(context, status=context.user_data.get("diesel_prihod_status") or "-"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="tz_diesel_hist_back|list")]])
        )
        return

    row = get_diesel_transfer_full_row(record_id)
    if not row:
        await query.message.reply_text("❌ Маълумот топилмади.")
        return
    video_id = row[8]
    if video_id:
        try:
            await query.message.chat.send_video_note(video_id)
        except Exception:
            try:
                await query.message.chat.send_video(video_id, caption="🎥 Дизел расход видео")
            except Exception:
                pass
    await query.message.chat.send_message(
        diesel_transfer_sender_card_text(row, status_text=diesel_status_display(row[9])),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Орқага", callback_data="tz_diesel_hist_back|list")]])
    )


def pending_repair_exit_count():
    try:
        count = 0
        for row in get_all_cars():
            if len(row) >= 7 and (row[6] or "").strip().lower() == "текширувда":
                count += 1
        return count
    except Exception as e:
        print("PENDING REPAIR EXIT COUNT ERROR:", e)
        return 0


def pending_diesel_prihod_count():
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM diesel_prihod
            WHERE TRIM(COALESCE(status, '')) = %s
        """, ("Текширувда",))
        row = cursor.fetchone()
        return int(row[0] or 0)
    except Exception as e:
        print("PENDING DIESEL PRIHOD COUNT ERROR:", e)
        return 0

def pending_registration_count():
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM drivers
            WHERE TRIM(COALESCE(status, '')) = %s
        """, ("Текширувда",))
        row = cursor.fetchone()
        return int(row[0] or 0)
    except Exception as e:
        print("PENDING REGISTRATION COUNT ERROR:", e)
        return 0


def technadzor_staff_menu_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🚚 Ҳайдовчилар")],
        [KeyboardButton("🔧 Механиклар")],
        [KeyboardButton("⛽ Заправщиклар")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def technadzor_staff_firms_reply_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(name)] for name in FIRM_NAMES] + [[KeyboardButton("⬅️ Орқага")]],
        resize_keyboard=True
    )


def technadzor_driver_firms_reply_keyboard():
    rows = []
    for firm in FIRM_NAMES:
        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM drivers
                WHERE COALESCE(work_role, 'driver') = 'driver'
                  AND status = 'Тасдиқланди'
                  AND firm = %s
            """, (firm,))
            count = int((cursor.fetchone() or [0])[0] or 0)
        except Exception as e:
            print("DRIVER FIRM COUNT ERROR:", e)
            count = 0
        rows.append([KeyboardButton(f"{firm} 👥 [ {count} ]")])

    rows.append([KeyboardButton("⬅️ Орқага")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def extract_firm_from_count_button(value):
    value = value or ""
    return value.split(" 👥 [", 1)[0].strip()


def technadzor_staff_back_reply_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("⬅️ Орқага")]], resize_keyboard=True)


def only_back_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("⬅️ Орқага")]], resize_keyboard=True)


def technadzor_staff_menu_inline_keyboard():
    # Эски callback структурани бузмаслик учун қолдирилди, лекин янги UXда меню пастки кнопкаларда.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 Ҳайдовчилар", callback_data="tz_staff|drivers")],
        [InlineKeyboardButton("🔧 Механиклар", callback_data="tz_staff|mechanics")],
        [InlineKeyboardButton("⛽ Заправщик", callback_data="tz_staff|zapravshik")],
    ])


def technadzor_staff_back_keyboard(back_to="staff_menu"):
    # Эски callback структурани бузмаслик учун қолдирилди.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Орқага", callback_data=f"tz_staff_back|{back_to}")]
    ])


def technadzor_staff_firms_keyboard(staff_type):
    # Эски callback структурани бузмаслик учун қолдирилди. Янги UXда фирмалар пастки кнопкада.
    role_map = {
        "drivers": "driver",
        "mechanics": "mechanic",
    }
    work_role = role_map.get(staff_type, "driver")

    try:
        cursor.execute("""
            SELECT DISTINCT firm
            FROM drivers
            WHERE COALESCE(work_role, 'driver') = %s
              AND status = 'Тасдиқланди'
              AND COALESCE(firm, '') <> ''
            ORDER BY firm
        """, (work_role,))
        firms = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print("TECHNADZOR STAFF FIRMS ERROR:", e)
        firms = []

    if not firms:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Маълумот йўқ", callback_data="none")],
            [InlineKeyboardButton("⬅️ Орқага", callback_data="tz_staff_back|staff_menu")]
        ])

    buttons = [[InlineKeyboardButton(firm, callback_data=f"tz_staff_firm|{staff_type}|{firm}")] for firm in firms]
    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data="tz_staff_back|staff_menu")])
    return InlineKeyboardMarkup(buttons)



def remember_inline_message(context, msg):
    try:
        if msg and getattr(msg, "message_id", None):
            ids = context.user_data.setdefault("all_inline_message_ids", [])
            if msg.message_id not in ids:
                ids.append(msg.message_id)
    except Exception as e:
        print("REMEMBER INLINE MESSAGE ERROR:", e)


def remember_inline_message_for_chat(context, chat_id, msg):
    """
    Бошқа user'га bot.send_message орқали юборилган inline кнопкали
    хабарларни ҳам кейин ўчириш учун сақлаб қўяди.
    """
    try:
        if not msg or not getattr(msg, "message_id", None) or not chat_id:
            return
        key = f"all_inline_message_ids:{int(chat_id)}"
        ids = context.bot_data.setdefault(key, [])
        if msg.message_id not in ids:
            ids.append(msg.message_id)
    except Exception as e:
        print("REMEMBER INLINE MESSAGE FOR CHAT ERROR:", e)


def remember_staff_inline_query_message(context, query):
    """Текширувчи ходимлар бўлимида edit қилинган inline хабар ID'сини сақлаб қўяди.
    Шунинг учун пастдаги Орқага ёки /start босилганда block/play/edit/delete кнопкалари қолиб кетмайди.
    """
    try:
        if query and query.message:
            msg_id = query.message.message_id
            context.user_data["technadzor_staff_inline_message_id"] = msg_id
            ids = context.user_data.setdefault("all_inline_message_ids", [])
            if msg_id not in ids:
                ids.append(msg_id)
            chat_id = query.message.chat_id
            key = f"all_inline_message_ids:{int(chat_id)}"
            bot_ids = context.bot_data.setdefault(key, [])
            if msg_id not in bot_ids:
                bot_ids.append(msg_id)
    except Exception as e:
        print("REMEMBER STAFF INLINE QUERY MESSAGE ERROR:", e)


async def clear_all_inline_messages(context, chat_id):
    try:
        ids = []
        ids.extend(list(context.user_data.get("all_inline_message_ids", []))[-5:])

        bot_data_key = f"all_inline_message_ids:{int(chat_id)}"
        ids.extend(list(context.bot_data.get(bot_data_key, []))[-5:])

        for key in ["technadzor_staff_message_id", "technadzor_staff_inline_message_id"]:
            value = context.user_data.get(key)
            if value:
                ids.append(value)

        clean_ids = []
        for value in ids:
            try:
                msg_id = int(value)
                if msg_id not in clean_ids:
                    clean_ids.append(msg_id)
            except Exception:
                pass

        for msg_id in clean_ids[-8:]:
            try:
                await context.bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=msg_id,
                    reply_markup=None
                )
            except Exception:
                pass

        context.user_data["all_inline_message_ids"] = []
        context.bot_data[bot_data_key] = []
    except Exception as e:
        print("CLEAR ALL INLINE MESSAGES ERROR:", e)


async def clear_technadzor_staff_inline(context, chat_id):
    await clear_all_inline_messages(context, chat_id)
    message_ids = []
    for key in ["technadzor_staff_message_id", "technadzor_staff_inline_message_id"]:
        value = context.user_data.get(key)
        if value:
            message_ids.append(value)

    for message_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=int(message_id))
        except Exception:
            try:
                await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=int(message_id), reply_markup=None)
            except Exception:
                pass

    context.user_data.pop("technadzor_staff_message_id", None)
    context.user_data.pop("technadzor_staff_inline_message_id", None)



async def safe_edit_message_text(query, text_value, reply_markup=None, parse_mode=None):
    """Telegram 'Message is not modified' хатосида бот йиқилмаслиги учун."""
    try:
        await query.edit_message_text(
            text_value,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return True
    except BadRequest as e:
        if "Message is not modified" in str(e):
            try:
                await query.answer()
            except Exception:
                pass
            return False
        print("SAFE EDIT MESSAGE TEXT BADREQUEST:", e)
        return False
    except Exception as e:
        print("SAFE EDIT MESSAGE TEXT ERROR:", e)
        return False


async def safe_clear_inline_keyboard(context, chat_id, message_id):
    """Inline keyboard'ни хавфсиз ўчиради: хабар топилмаса ҳам бот йиқилмайди."""
    try:
        if not chat_id or not message_id:
            return False
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=int(message_id),
            reply_markup=None
        )
        return True
    except BadRequest as e:
        text = str(e)
        if (
            "Message is not modified" in text
            or "message to edit not found" in text
            or "message can't be edited" in text
            or "message is not modified" in text.lower()
        ):
            return False
        print("SAFE CLEAR INLINE BADREQUEST:", e)
        return False
    except Exception as e:
        print("SAFE CLEAR INLINE ERROR:", e)
        return False


def get_car_type_by_number(car_number):
    if not car_number:
        return ""

    cache_key = f"car_type_by_number:{str(car_number).strip().lower()}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT car_type
            FROM cars
            WHERE LOWER(car_number) = LOWER(%s)
            LIMIT 1
        """, (car_number,))
        row = cursor.fetchone()
        if row:
            return cache_set(cache_key, row[0] or "")
        return cache_set(cache_key, "")
    except Exception as e:
        print("GET CAR TYPE ERROR:", e)

    return ""


def staff_status_label(status):
    if status == "Тасдиқланди":
        return "PLAY"
    if status == "Рад этилди":
        return "BLOCK"
    if status == "Текширувда":
        return "Текширувда"
    return status or "Текширувда"


def staff_status_db(label):
    return "Тасдиқланди" if label == "PLAY" else "Рад этилди"


def staff_short_name(name, surname):
    name = (name or "").strip()
    surname = (surname or "").strip()
    if surname and name:
        return f"{surname} {name[:1]}."
    return surname or name or "Номаълум"


def get_staff_type_from_work_role(work_role):
    if work_role == "mechanic":
        return "mechanics"
    if work_role == "zapravshik":
        return "zapravshik"
    return "drivers"


def get_staff_role_from_type(staff_type):
    return {
        "drivers": "driver",
        "mechanics": "mechanic",
        "zapravshik": "zapravshik",
    }.get(staff_type, "driver")


def get_staff_title(staff_type):
    return {
        "drivers": "🚚 Ҳайдовчилар",
        "mechanics": "🔧 Механиклар",
        "zapravshik": "⛽ Заправщиклар",
    }.get(staff_type, "👥 Ходимлар")


def get_staff_by_id(driver_id):
    try:
        cursor.execute("""
            SELECT id, telegram_id, name, surname, phone, firm, car, status,
                   COALESCE(work_role, 'driver')
            FROM drivers
            WHERE id = %s
            LIMIT 1
        """, (int(driver_id),))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "telegram_id": row[1],
            "name": row[2] or "",
            "surname": row[3] or "",
            "phone": row[4] or "",
            "firm": row[5] or "",
            "car": row[6] or "",
            "status": row[7] or "Рад этилди",
            "work_role": row[8] or "driver",
        }
    except Exception as e:
        print("GET STAFF BY ID ERROR:", e)
        return None



def update_driver_status_in_google_sheet(telegram_id, status):
    try:
        rows = drivers_ws.get_all_values()
        target = str(telegram_id)

        for idx, row in enumerate(rows, start=1):
            if not row:
                continue

            if str(row[0]).strip() == target:
                # DRIVERS лист структураси: A telegram_id ... G status
                drivers_ws.update_cell(idx, 7, status)
                return True

        return False

    except Exception as e:
        print("UPDATE DRIVER STATUS IN SHEET ERROR:", e)
        return False


def delete_driver_from_google_sheet(telegram_id):
    try:
        rows = drivers_ws.get_all_values()
        target = str(telegram_id)

        for idx, row in enumerate(rows, start=1):
            if not row:
                continue

            if str(row[0]).strip() == target:
                drivers_ws.delete_rows(idx)
                return True

        return False

    except Exception as e:
        print("DELETE DRIVER FROM SHEET ERROR:", e)
        return False


def sync_car_status_to_google_sheet(car_number, status):
    """SQL cars.status ўзгарса, MASHINALAR листидаги status ҳам янгиланади.
    MASHINALAR структураси: B = car_number, G = status.
    Google Sheets хатоси ботни тўхтатмаслиги керак.
    """
    try:
        if not car_number:
            return False

        rows = mashina_ws.get_all_values()
        target = str(car_number).strip().lower()

        for idx, row in enumerate(rows, start=1):
            if len(row) > 1 and str(row[1]).strip().lower() == target:
                mashina_ws.update_cell(idx, 7, status or "")
                return True

        return False
    except Exception as e:
        print("SYNC CAR STATUS TO SHEET ERROR:", e)
        return False


def sync_driver_to_google_sheet(driver_id=None, telegram_id=None):
    """SQL drivers маълумоти ўзгарса, DRIVERS листидаги шу ходим қатори ҳам янгиланади.
    DRIVERS структураси: A telegram_id, B name, C surname, D phone, E firm, F car, G status, H date, I work_role.
    Google Sheets хатоси ботни тўхтатмаслиги керак.
    """
    try:
        if driver_id is not None:
            cursor.execute("""
                SELECT telegram_id, name, surname, phone, firm, car, status, COALESCE(work_role, 'driver')
                FROM drivers
                WHERE id = %s
                LIMIT 1
            """, (int(driver_id),))
        elif telegram_id is not None:
            cursor.execute("""
                SELECT telegram_id, name, surname, phone, firm, car, status, COALESCE(work_role, 'driver')
                FROM drivers
                WHERE telegram_id = %s
                LIMIT 1
            """, (int(telegram_id),))
        else:
            return False

        db_row = cursor.fetchone()
        if not db_row:
            return False

        tg_id, name, surname, phone, firm, car, status, work_role = db_row
        sheet_row = [
            tg_id or "",
            name or "",
            surname or "",
            phone or "",
            firm or "",
            car or "",
            status or "",
            now_text(),
            work_role or "driver",
        ]

        rows = drivers_ws.get_all_values()
        target = str(tg_id).strip()

        for idx, row in enumerate(rows, start=1):
            if len(row) > 0 and str(row[0]).strip() == target:
                drivers_ws.update(f"A{idx}:I{idx}", [sheet_row])
                return True

        drivers_ws.append_row(sheet_row)
        return True
    except Exception as e:
        print("SYNC DRIVER TO SHEET ERROR:", e)
        return False


async def clear_blocked_user_bot_messages(context, telegram_id):
    try:
        # Бот юборган охирги маълум inline/хабарларни ўчиришга ҳаракат қилади.
        for key in [
            "all_inline_message_ids",
            "bot_message_ids",
            "technadzor_staff_message_id",
            "technadzor_staff_inline_message_id",
            "registration_message_id",
            "confirm_message_id",
        ]:
            value = context.user_data.get(key)

            if isinstance(value, list):
                for message_id in value:
                    try:
                        await context.bot.delete_message(chat_id=int(telegram_id), message_id=int(message_id))
                    except Exception:
                        try:
                            await context.bot.edit_message_reply_markup(
                                chat_id=int(telegram_id),
                                message_id=int(message_id),
                                reply_markup=None
                            )
                        except Exception:
                            pass

            elif value:
                try:
                    await context.bot.delete_message(chat_id=int(telegram_id), message_id=int(value))
                except Exception:
                    try:
                        await context.bot.edit_message_reply_markup(
                            chat_id=int(telegram_id),
                            message_id=int(value),
                            reply_markup=None
                        )
                    except Exception:
                        pass

    except Exception as e:
        print("CLEAR BLOCKED USER BOT MESSAGES ERROR:", e)


async def notify_staff_blocked(context, telegram_id):
    try:
        await context.bot.send_message(
            chat_id=int(telegram_id),
            text="⛔ Сиз блокландингиз. Ботдан фойдаланиш вақтинча тўхтатилди.",
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception as e:
        print("NOTIFY STAFF BLOCKED ERROR:", e)


async def notify_staff_play(context, telegram_id):
    try:
        await context.bot.send_message(
            chat_id=int(telegram_id),
            text="✅ Сизга ботдан фойдаланиш рухсати берилди. /start босинг.",
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception as e:
        print("NOTIFY STAFF PLAY ERROR:", e)


def staff_list_button_text(row):
    driver_id, name, surname, firm, car, status, work_role = row
    short_name = staff_short_name(name, surname)
    status_text = staff_status_label(status)

    if (work_role or "driver") == "driver":
        return " / ".join([x for x in [short_name, car or "Техника йўқ", status_text] if x])

    return " / ".join([x for x in [short_name, status_text] if x])


def technadzor_staff_list_text(staff_type, firm=None):
    title = get_staff_title(staff_type)
    if firm:
        return f"{title}\n🏢 {firm}\n\nКеракли ходимни рўйхатдан танланг."
    return f"{title}\n\nКеракли ходимни рўйхатдан танланг."


def technadzor_staff_list_inline_keyboard(staff_type, firm=None):
    work_role = get_staff_role_from_type(staff_type)

    params = [work_role]
    firm_filter = ""
    if firm:
        firm_filter = "AND firm = %s"
        params.append(firm)

    try:
        cursor.execute(f"""
            SELECT id, name, surname, firm, car, status, COALESCE(work_role, 'driver')
            FROM drivers
            WHERE COALESCE(work_role, 'driver') = %s
              {firm_filter}
              AND COALESCE(status, '') IN ('Тасдиқланди', 'Рад этилди')
            ORDER BY firm, surname, name
        """, tuple(params))
        rows = cursor.fetchall()
    except Exception as e:
        print("TECHNADZOR STAFF INLINE LIST ERROR:", e)
        rows = []

    if not rows:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Маълумот топилмади", callback_data="none")]])

    buttons = []
    current_firm = None

    for row in rows:
        driver_id, name, surname, firm_name, car, status, work_role = row
        if staff_type == "mechanics" and firm_name != current_firm:
            current_firm = firm_name
            buttons.append([InlineKeyboardButton(f"🏢 {firm_name or 'Фирма йўқ'}", callback_data="none")])

        button_text = staff_list_button_text(row)
        buttons.append([InlineKeyboardButton(button_text[:60], callback_data=f"tz_staff_view|{driver_id}")])

    return InlineKeyboardMarkup(buttons)



def technadzor_pending_registration_keyboard():
    try:
        cursor.execute("""
            SELECT id, name, surname, firm, car, status, COALESCE(work_role, 'driver')
            FROM drivers
            WHERE TRIM(COALESCE(status, '')) = 'Текширувда'
            ORDER BY
                CASE
                    WHEN COALESCE(work_role, 'driver') = 'zapravshik' THEN 'Заправщик'
                    ELSE COALESCE(NULLIF(firm, ''), 'Фирма йўқ')
                END,
                surname,
                name
        """)
        rows = cursor.fetchall()
    except Exception as e:
        print("TECHNADZOR PENDING REGISTRATION ERROR:", e)
        rows = []

    if not rows:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Текширувда ходим йўқ", callback_data="none")]])

    buttons = []
    current_group = None

    for row in rows:
        driver_id, name, surname, firm, car, status, work_role = row
        group_name = "Заправщик" if (work_role or "driver") == "zapravshik" else (firm or "Фирма йўқ")

        if group_name != current_group:
            current_group = group_name
            buttons.append([InlineKeyboardButton(f"🏢 {group_name}", callback_data="none")])

        buttons.append([
            InlineKeyboardButton(
                staff_list_button_text(row)[:60],
                callback_data=f"tz_reg_view|{driver_id}"
            )
        ])

    return InlineKeyboardMarkup(buttons)


def technadzor_pending_registration_card_keyboard(driver_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"tz_reg_approve|{driver_id}"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"tz_staff_edit|{driver_id}"),
        ],
        [InlineKeyboardButton("❌ Рад этиш", callback_data=f"tz_reg_reject|{driver_id}")]
    ])


def technadzor_staff_card_text(driver_id):
    staff = get_staff_by_id(driver_id)
    if not staff:
        return "❌ Ходим топилмади."

    work_role = staff.get("work_role") or "driver"

    text = (
        "👤 Ходим маълумоти\n\n"
        f"👤 Лавозим: {work_role_title(work_role)}\n"
        f"👤 Исм: {staff['name'] or '-'}\n"
        f"👤 Фамилия: {staff['surname'] or '-'}\n"
        f"📞 Телефон: {staff['phone'] or '-'}\n"
    )

    if work_role in ["driver", "mechanic"]:
        text += f"🏢 Фирма: {staff['firm'] or '-'}\n"

    if work_role == "driver":
        text += f"🚛 Техника: {staff['car'] or '-'}\n"

    text += f"📌 Ҳолати: {staff_status_label(staff['status'])}"
    return text


def technadzor_staff_card_keyboard(driver_id):
    staff = get_staff_by_id(driver_id)
    if not staff:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Ходим топилмади", callback_data="none")]])

    status_label = staff_status_label(staff["status"])
    toggle_text = "▶️ PLAY" if status_label == "BLOCK" else "⛔ BLOCK"
    toggle_to = "PLAY" if status_label == "BLOCK" else "BLOCK"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"tz_staff_edit|{driver_id}")],
        [InlineKeyboardButton(toggle_text, callback_data=f"tz_staff_status|{driver_id}|{toggle_to}")],
        [InlineKeyboardButton("🗑 Delete", callback_data=f"tz_staff_delete|{driver_id}")],
    ])


def technadzor_staff_edit_keyboard(driver_id):
    staff = get_staff_by_id(driver_id)
    work_role = (staff or {}).get("work_role", "driver")

    buttons = [
        [InlineKeyboardButton("👤 Исм", callback_data=f"tz_staff_edit_field|{driver_id}|name")],
        [InlineKeyboardButton("👤 Фамилия", callback_data=f"tz_staff_edit_field|{driver_id}|surname")],
        [InlineKeyboardButton("📞 Телефон", callback_data=f"tz_staff_edit_field|{driver_id}|phone")],
        [InlineKeyboardButton("🪪 Лавозим", callback_data=f"tz_staff_edit_field|{driver_id}|role")],
    ]

    if work_role in ["driver", "mechanic"]:
        buttons.append([InlineKeyboardButton("🏢 Фирма", callback_data=f"tz_staff_edit_field|{driver_id}|firm")])

    if work_role == "driver":
        buttons.append([InlineKeyboardButton("🚛 Техника", callback_data=f"tz_staff_edit_field|{driver_id}|car")])

    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data="tz_staff_action_back")])
    return InlineKeyboardMarkup(buttons)




def technadzor_pending_edit_backup_key(driver_id):
    return str(driver_id)


def technadzor_save_pending_edit_backup(context, driver_id):
    staff = get_staff_by_id(driver_id)
    if not staff or staff.get("status") != "Текширувда":
        return

    backups = context.user_data.setdefault("technadzor_pending_edit_backups", {})
    key = technadzor_pending_edit_backup_key(driver_id)

    if key not in backups:
        backups[key] = {
            "name": staff.get("name", ""),
            "surname": staff.get("surname", ""),
            "phone": staff.get("phone", ""),
            "firm": staff.get("firm", ""),
            "car": staff.get("car", ""),
            "status": staff.get("status", "Текширувда"),
            "work_role": staff.get("work_role", "driver"),
        }


def technadzor_clear_pending_edit_backup(context, driver_id):
    backups = context.user_data.get("technadzor_pending_edit_backups", {})
    backups.pop(technadzor_pending_edit_backup_key(driver_id), None)


def technadzor_rollback_pending_edit(context, driver_id):
    backups = context.user_data.get("technadzor_pending_edit_backups", {})
    key = technadzor_pending_edit_backup_key(driver_id)
    backup = backups.get(key)

    if not backup:
        return False

    try:
        cursor.execute("""
            UPDATE drivers
            SET name = %s,
                surname = %s,
                phone = %s,
                firm = %s,
                car = %s,
                status = %s,
                work_role = %s
            WHERE id = %s
        """, (
            backup.get("name", ""),
            backup.get("surname", ""),
            backup.get("phone", ""),
            backup.get("firm", ""),
            backup.get("car", ""),
            backup.get("status", "Текширувда"),
            backup.get("work_role", "driver"),
            int(driver_id),
        ))
        conn.commit()
        backups.pop(key, None)
        return True
    except Exception as e:
        print("TECHNADZOR PENDING EDIT ROLLBACK ERROR:", e)
        return False


async def technadzor_show_pending_or_staff_card(message, context, driver_id, text_prefix=None):
    context.user_data["mode"] = "technadzor_staff_card"
    context.user_data["technadzor_selected_staff_id"] = str(driver_id)

    if text_prefix:
        await message.reply_text(text_prefix, reply_markup=technadzor_staff_back_reply_keyboard())

    msg = await message.reply_text(
        technadzor_staff_card_text(driver_id),
        reply_markup=technadzor_staff_card_reply_markup(driver_id)
    )
    context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
    remember_inline_message(context, msg)
    return msg




def technadzor_normalize_staff_fields_for_role(driver_id, work_role):
    """
    Лавозимга кераксиз майдонларни базадан тозалайди.
    driver   -> firm/car кейин алоҳида танланади
    mechanic -> firm керак, car керак эмас
    zapravshik -> firm/car керак эмас
    """
    try:
        if work_role == "zapravshik":
            cursor.execute(
                "UPDATE drivers SET work_role = %s, firm = NULL, car = NULL WHERE id = %s",
                (work_role, int(driver_id))
            )
        elif work_role == "mechanic":
            cursor.execute(
                "UPDATE drivers SET work_role = %s, car = NULL WHERE id = %s",
                (work_role, int(driver_id))
            )
        elif work_role == "driver":
            cursor.execute(
                "UPDATE drivers SET work_role = %s, firm = NULL, car = NULL WHERE id = %s",
                (work_role, int(driver_id))
            )
        conn.commit()
        sync_driver_to_google_sheet(driver_id=driver_id)
        return True
    except Exception as e:
        print("TECHNADZOR NORMALIZE ROLE FIELDS ERROR:", e)
        return False


def technadzor_pending_decision_done(context, driver_id):
    done = context.user_data.setdefault("technadzor_pending_decision_done", {})
    done[str(driver_id)] = True


def technadzor_is_pending_decision_done(context, driver_id):
    return context.user_data.get("technadzor_pending_decision_done", {}).get(str(driver_id), False)


def technadzor_clear_pending_decision_done(context, driver_id):
    context.user_data.get("technadzor_pending_decision_done", {}).pop(str(driver_id), None)


def get_staff_by_telegram_id(telegram_id):
    try:
        cursor.execute("""
            SELECT id, telegram_id, name, surname, phone, firm, car, status,
                   COALESCE(work_role, 'driver')
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(telegram_id),))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "telegram_id": row[1],
            "name": row[2] or "",
            "surname": row[3] or "",
            "phone": row[4] or "",
            "firm": row[5] or "",
            "car": row[6] or "",
            "status": row[7] or "Рад этилди",
            "work_role": row[8] or "driver",
        }
    except Exception as e:
        print("GET STAFF BY TELEGRAM ID ERROR:", e)
        return None


def sync_driver_status_to_sheet(telegram_id, status):
    try:
        rows = drivers_ws.get_all_values()
        for i, row in enumerate(rows, start=1):
            if len(row) > 0 and str(row[0]) == str(telegram_id):
                drivers_ws.update_cell(i, 7, status)
    except Exception as e:
        print("SYNC DRIVER STATUS TO SHEET ERROR:", e)


async def notify_registered_employee(context, telegram_id, status, work_role):
    try:
        if status == "Тасдиқланди":
            if work_role == "mechanic":
                text = "✅ Механик сифатида маълумотларингиз тасдиқланди.\n/start босинг."
            elif work_role == "zapravshik":
                text = "✅ Заправщик сифатида маълумотларингиз тасдиқланди.\n/start босинг."
            else:
                text = "✅ Ҳайдовчи сифатида маълумотларингиз тасдиқланди.\n/start босинг."
        else:
            text = "❌ Маълумотларингиз рад этилди.\nАдминистратор билан боғланинг."

        await context.bot.send_message(chat_id=int(telegram_id), text=text)
    except Exception as e:
        print("NOTIFY REGISTERED EMPLOYEE ERROR:", e)


def technadzor_staff_card_reply_markup(driver_id):
    staff = get_staff_by_id(driver_id)
    if staff and staff.get("status") == "Текширувда":
        return technadzor_pending_registration_card_keyboard(driver_id)
    return technadzor_staff_card_keyboard(driver_id)


def technadzor_staff_role_reply_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🚚 Ҳайдовчи")],
        [KeyboardButton("🔧 Механик")],
        [KeyboardButton("⛽ Заправщик")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)




SPECIAL_MULTI_DRIVER_CAR_NORM = "30615RBA"

def normalize_car_for_driver_limit(car_number):
    return re.sub(r"\s+", "", str(car_number or "")).upper()

def get_driver_car_counts_by_firm(firm, exclude_driver_id=None):
    """Фирма бўйича техникага бириктирилган ҳайдовчилар сони.
    30 615 RBA учун 2 та ҳайдовчига рухсат берилади, қолган техникага 1 та.
    """
    counts = {}
    try:
        params = [firm]
        exclude_sql = ""
        if exclude_driver_id:
            exclude_sql = " AND id <> %s"
            params.append(int(exclude_driver_id))

        cursor.execute(f"""
            SELECT UPPER(REPLACE(COALESCE(car, ''), ' ', '')) AS car_norm, COUNT(*)
            FROM drivers
            WHERE firm = %s
              AND COALESCE(work_role, 'driver') = 'driver'
              AND COALESCE(car, '') <> ''
              AND COALESCE(status, '') <> 'Рад этилди'
              {exclude_sql}
            GROUP BY UPPER(REPLACE(COALESCE(car, ''), ' ', ''))
        """, tuple(params))
        counts = {row[0]: int(row[1]) for row in cursor.fetchall() if row and row[0]}
    except Exception as e:
        print("GET DRIVER CAR COUNTS ERROR:", e)
    return counts

def is_car_available_for_driver(car_number, counts):
    car_norm = normalize_car_for_driver_limit(car_number)
    used_count = counts.get(car_norm, 0)
    if car_norm == SPECIAL_MULTI_DRIVER_CAR_NORM:
        return used_count < 2
    return used_count == 0

def technadzor_staff_cars_reply_keyboard(firm, exclude_driver_id=None):
    try:
        cursor.execute("""
            SELECT car_number
            FROM cars
            WHERE firm = %s
            ORDER BY car_number
        """, (firm,))
        all_cars = [row[0] for row in cursor.fetchall() if row[0]]
        counts = get_driver_car_counts_by_firm(firm, exclude_driver_id=exclude_driver_id)
        cars = [car for car in all_cars if is_car_available_for_driver(car, counts)]
    except Exception as e:
        print("TECHNADZOR STAFF CARS REPLY ERROR:", e)
        cars = []

    rows = [[KeyboardButton(car)] for car in cars]
    rows.append([KeyboardButton("⬅️ Орқага")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def technadzor_staff_show_list_keyboard_context(context, staff_type=None, firm=None):
    if staff_type is None:
        staff_type = context.user_data.get("technadzor_staff_type", "drivers")
    if firm is None:
        firm = context.user_data.get("technadzor_staff_firm")
    return technadzor_staff_list_inline_keyboard(staff_type, firm)


def repair_type_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(name)] for name in REPAIR_TYPES] + [[KeyboardButton("⬅️ Орқага")]],
        resize_keyboard=True
    )


def back_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⬅️ Орқага")]
    ], resize_keyboard=True)

def phone_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📞 Телефонни юбориш", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def phone_back_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📞 Телефонни юбориш", request_contact=True)],
            [KeyboardButton("⬅️ Орқага")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def driver_main_keyboard(fuel_type=""):
    buttons = [
        [KeyboardButton("⛽ Ёқилғи ҳисоботи")],
        [KeyboardButton("📊 Ҳисоботлар")],
        [KeyboardButton("📄 Техника ҳужжатлари")],
        [KeyboardButton("📦 Инвентар")],
    ]

    if fuel_type.lower() == "газ":
        buttons.insert(1, [KeyboardButton("🟢 Газ баллон маълумоти")])

    if fuel_type.lower() == "дизел":
        buttons.insert(1, [KeyboardButton("🟡 Дизел лимит маълумоти")])

    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def gas_report_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⛽ ГАЗ олиш")],
        [KeyboardButton("⛽ ГАЗ бериш")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def get_diesel_give_status_count(user_id):
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND TRIM(COALESCE(status, '')) IN ('Қабул қилувчи текширувида', 'Рад этилди', 'Қайтди', 'Кайтарилди')
        """, (int(user_id),))
        row = cursor.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception as e:
        print("DIESEL GIVE STATUS COUNT ERROR:", e)
        return 0


def diesel_report_keyboard(user_id=None):
    status_count = get_diesel_give_status_count(user_id) if user_id else 0
    return ReplyKeyboardMarkup([
        [KeyboardButton("✅ ДИЗЕЛ приход")],
        [KeyboardButton("⛽ ДИЗЕЛ расход")],
        [KeyboardButton(f"📌 ДИЗЕЛ бериш статуси ({status_count} та)")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def diesel_give_status_keyboard(user_id):
    buttons = []

    try:
        cursor.execute("""
            SELECT id, to_car, liter, status, created_at
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND TRIM(COALESCE(status, '')) = 'Қабул қилувчи текширувида'
            ORDER BY created_at DESC
        """, (int(user_id),))
        pending_rows = cursor.fetchall()
    except Exception as e:
        print("DIESEL GIVE PENDING STATUS LIST ERROR:", e)
        pending_rows = []

    buttons.append([InlineKeyboardButton("⏳ Тасдиқлашда", callback_data="none")])
    if pending_rows:
        for transfer_id, to_car, liter, status, created_at in pending_rows:
            status_text = diesel_status_display(status) if 'diesel_status_display' in globals() else (status or '-')
            liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter or '-')
            buttons.append([
                InlineKeyboardButton(
                    f"{liter_text} л / {to_car or '-'} / {status_text}"[:60],
                    callback_data=f"znotif_diesel_pending|{transfer_id}"
                )
            ])
    else:
        buttons.append([InlineKeyboardButton("Маълумот йўқ", callback_data="none")])

    try:
        cursor.execute("""
            SELECT id, to_car, liter, status, created_at
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND TRIM(COALESCE(status, '')) IN ('Рад этилди', 'Қайтди', 'Кайтарилди')
            ORDER BY created_at DESC
        """, (int(user_id),))
        rejected_rows = cursor.fetchall()
    except Exception as e:
        print("DIESEL GIVE REJECTED STATUS LIST ERROR:", e)
        rejected_rows = []

    buttons.append([InlineKeyboardButton("❌ Рад этилган", callback_data="none")])
    if rejected_rows:
        for transfer_id, to_car, liter, status, created_at in rejected_rows:
            status_text = diesel_status_display(status) if 'diesel_status_display' in globals() else (status or '-')
            liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter or '-')
            buttons.append([
                InlineKeyboardButton(
                    f"{liter_text} л / {to_car or '-'} / {status_text}"[:60],
                    callback_data=f"znotif_diesel_rejected|{transfer_id}"
                )
            ])
    else:
        buttons.append([InlineKeyboardButton("Маълумот йўқ", callback_data="none")])

    return InlineKeyboardMarkup(buttons)



def to_float_liter(value):
    try:
        text_value = str(value or "0").replace(",", ".")
        numbers = re.findall(r"\d+(?:\.\d+)?", text_value)
        if not numbers:
            return 0.0
        return float(numbers[0])
    except Exception:
        return 0.0


def format_liter(value):
    try:
        value = float(value or 0)
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


def get_all_diesel_stock_by_firm_cached():
    cache_key = "diesel:stock_by_firm:all"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    data = {firm: {"prihod": 0.0, "rashod": 0.0} for firm in FIRM_NAMES}

    try:
        firm_map = {firm.strip().lower(): firm for firm in FIRM_NAMES}

        cursor.execute("""
            SELECT liter, note
            FROM diesel_prihod
            WHERE TRIM(COALESCE(status, '')) = 'Тасдиқланди'
        """)
        for liter, note in cursor.fetchall():
            firm_text, _, _ = parse_diesel_prihod_note(note or "")
            firm = firm_map.get((firm_text or "").strip().lower())
            if firm:
                data[firm]["prihod"] += to_float_liter(liter)

        cursor.execute("""
            SELECT COALESCE(firm, ''), COALESCE(SUM(
                CASE
                    WHEN liter ~ '^[0-9]+([.][0-9]+)?$'
                    THEN liter::numeric
                    ELSE 0
                END
            ), 0)
            FROM diesel_transfers
            WHERE TRIM(COALESCE(status, '')) IN ('Тасдиқланди', 'Берилди')
              AND TRIM(COALESCE(from_car, '')) = 'Заправщик'
            GROUP BY COALESCE(firm, '')
        """)
        for firm_text, total in cursor.fetchall():
            firm = firm_map.get((firm_text or "").strip().lower())
            if firm:
                data[firm]["rashod"] = float(total or 0)
    except Exception as e:
        print("GET ALL DIESEL STOCK ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass

    return cache_set(cache_key, data)


def clear_diesel_stock_cache():
    clear_speed_cache("diesel:stock_by_firm:")
    clear_speed_cache("zapravka:info_text")
    clear_speed_cache("diesel:other_expense_total")
    clear_speed_cache("keyboard:diesel_prihod_firm_stock")


def get_diesel_prihod_sum_by_firm(firm):
    data = get_all_diesel_stock_by_firm_cached()
    return float(data.get(firm, {}).get("prihod", 0) or 0)


def get_diesel_rashod_sum_by_firm(firm):
    data = get_all_diesel_stock_by_firm_cached()
    return float(data.get(firm, {}).get("rashod", 0) or 0)


def get_total_company_diesel_stock():
    total = 0.0
    try:
        data = get_all_diesel_stock_by_firm_cached()
        for firm in FIRM_NAMES:
            prihod = float(data.get(firm, {}).get("prihod", 0) or 0)
            rashod = float(data.get(firm, {}).get("rashod", 0) or 0)
            total += prihod - rashod
    except Exception as e:
        print("TOTAL COMPANY DIESEL STOCK ERROR:", e)
    return total


def can_spend_diesel_amount(liter):
    total_stock = get_total_company_diesel_stock()
    spend = to_float_liter(liter)
    return total_stock >= spend and total_stock > 0, total_stock, spend


def is_zapravshik_diesel_expense_flow(context):
    return context.user_data.get("dieselgive_from_car") == "Заправщик"


def get_diesel_stock_by_firm(firm):
    data = get_all_diesel_stock_by_firm_cached()
    item = data.get(firm, {})
    prihod = float(item.get("prihod", 0) or 0)
    rashod = float(item.get("rashod", 0) or 0)
    return prihod, rashod, prihod - rashod

def diesel_prihod_firm_stock_keyboard():
    # Заправшик дизел менюсида фирмалар олдида остатка кўринади.
    # 45 сониялик cache меню очилишини тезлатади, статус/DB flow'га тегмайди.
    cache_key = "keyboard:diesel_prihod_firm_stock"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    rows = []
    for firm in FIRM_NAMES:
        _, _, ostatka = get_diesel_stock_by_firm(firm)
        rows.append([KeyboardButton(f"{firm} [ост:{format_liter(ostatka)} л]")])

    rows.append([KeyboardButton(f"📦 Бошқа дизел расходлар [ост:-{format_liter(get_other_diesel_expense_total())} л]")])
    rows.append([KeyboardButton("⬅️ Орқага")])
    return cache_set(cache_key, ReplyKeyboardMarkup(rows, resize_keyboard=True))


def diesel_firm_plain_keyboard():
    # Ҳайдовчи ролида фирмалар олдида остатка кўринмайди.
    cache_key = "keyboard:diesel_firm_plain"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    rows = [[KeyboardButton(firm)] for firm in FIRM_NAMES]
    rows.append([KeyboardButton("⬅️ Орқага")])
    return cache_set(cache_key, ReplyKeyboardMarkup(rows, resize_keyboard=True))


def extract_firm_from_stock_button(value):
    value = value or ""
    return value.split(" [ост:", 1)[0].strip()


def get_other_diesel_expense_total():
    cache_key = "diesel:other_expense_total"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN liter ~ '^[0-9]+([.][0-9]+)?$'
                    THEN liter::numeric
                    ELSE 0
                END
            ), 0)
            FROM diesel_other_expense
            WHERE TRIM(COALESCE(status, '')) = 'Тасдиқланди'
        """)
        row = cursor.fetchone()
        return cache_set(cache_key, float(row[0] or 0))
    except Exception as e:
        print("OTHER DIESEL TOTAL ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0.0


def zapravka_info_text():
    cache_key = "zapravka:info_text"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    lines = ["⛽ ЗАПРАВКА МАЬЛУМОТИ", ""]
    total_prihod = 0.0
    total_rashod = 0.0
    total_ostatka = 0.0
    stock_data = get_all_diesel_stock_by_firm_cached()

    for firm in FIRM_NAMES:
        item = stock_data.get(firm, {})
        prihod = float(item.get("prihod", 0) or 0)
        rashod = float(item.get("rashod", 0) or 0)
        ostatka = prihod - rashod
        total_prihod += prihod
        total_rashod += rashod
        total_ostatka += ostatka

        lines.append(firm)
        lines.append(
            f"Приход: {format_liter(prihod)}л / "
            f"Расход: {format_liter(rashod)}л / "
            f"Остатка: {format_liter(ostatka)}л"
        )
        lines.append("")

    other_total = get_other_diesel_expense_total()
    total_rashod += float(other_total or 0)
    total_ostatka -= float(other_total or 0)

    lines.append("📦 Бошқа дизел расходлар")
    lines.append(f"Остатка: -{format_liter(other_total)}л")
    lines.append("--------------------------------")
    lines.append("✅ ИТОГО:")
    lines.append(
        f"Приход: {format_liter(total_prihod)}л / "
        f"Расход: {format_liter(total_rashod)}л / "
        f"Остатка: {format_liter(total_ostatka)}л"
    )
    return cache_set(cache_key, "\n".join(lines).strip())

def register_role_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🔧 Механик")],
        [KeyboardButton("⛽ Заправщик")],
        [KeyboardButton("🚚 Ҳайдовчи")],
    ], resize_keyboard=True)


def zapravshik_main_keyboard():
    cached = cache_get("keyboard:zapravshik_main")
    if cached is not None:
        return cached
    return cache_set("keyboard:zapravshik_main", ReplyKeyboardMarkup([
        [KeyboardButton("🟡 Дизел бўлими")],
        [KeyboardButton("📊 Ҳисобот")],
    ], resize_keyboard=True))


def zapravshik_diesel_menu_keyboard():
    cached = cache_get("keyboard:zapravshik_diesel_menu")
    if cached is not None:
        return cached
    return cache_set("keyboard:zapravshik_diesel_menu", ReplyKeyboardMarkup([
        [KeyboardButton("➕ Дизел приход")],
        [KeyboardButton("➖ Дизел расход")],
        [KeyboardButton("🔔 Уведомления")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True))


def zapravshik_diesel_notifications_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📩 Приход/Расход хабарлари")],
        [KeyboardButton("⏳ Тасдиқлаш ҳолати")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)




def zapravshik_rejected_diesel_notifications_keyboard(user_id):
    buttons = []

    try:
        cursor.execute("""
            SELECT id, liter, note, status, created_at
            FROM diesel_prihod
            WHERE telegram_id = %s
              AND TRIM(COALESCE(status, '')) IN ('Қайтди', 'Кайтарилди', 'Рад этилди')
            ORDER BY created_at DESC
        """, (int(user_id),))
        for record_id, liter, note, status, created_at in cursor.fetchall():
            firm_text, _, _ = parse_diesel_prihod_note(note or "")
            date_text = created_at.strftime("%d.%m %H:%M") if created_at else ""
            buttons.append([
                InlineKeyboardButton(
                    f"➕ Приход | {date_text} | {liter} л | {firm_text or '-'}"[:60],
                    callback_data=f"znotif_prihod_return|{record_id}"
                )
            ])
    except Exception as e:
        print("ZAPRAVSHIK REJECTED PRIHOD LIST ERROR:", e)

    try:
        cursor.execute("""
            SELECT id, to_car, firm, liter, status, created_at
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND TRIM(COALESCE(status, '')) IN ('Рад этилди', 'Қайтди', 'Кайтарилди')
            ORDER BY created_at DESC
        """, (int(user_id),))
        for transfer_id, to_car, firm, liter, status, created_at in cursor.fetchall():
            date_text = created_at.strftime("%d.%m %H:%M") if created_at else ""
            buttons.append([
                InlineKeyboardButton(
                    f"➖ Расход | {date_text} | {liter} л | {to_car or '-'}"[:60],
                    callback_data=f"znotif_diesel_rejected|{transfer_id}"
                )
            ])
    except Exception as e:
        print("ZAPRAVSHIK REJECTED DIESEL LIST ERROR:", e)

    if not buttons:
        buttons.append([InlineKeyboardButton("✅ Рад этилган дизел хабарлари йўқ", callback_data="none")])

    return InlineKeyboardMarkup(buttons)


def zapravshik_pending_diesel_notifications_keyboard(user_id):
    buttons = []

    try:
        cursor.execute("""
            SELECT id, liter, note, status, created_at
            FROM diesel_prihod
            WHERE telegram_id = %s
              AND TRIM(COALESCE(status, '')) = 'Текширувда'
            ORDER BY created_at DESC
        """, (int(user_id),))
        for record_id, liter, note, status, created_at in cursor.fetchall():
            firm_text, _, _ = parse_diesel_prihod_note(note or "")
            date_text = created_at.strftime("%d.%m %H:%M") if created_at else ""
            buttons.append([
                InlineKeyboardButton(
                    f"➕ Приход | {date_text} | {liter} л | {firm_text or '-'}"[:60],
                    callback_data=f"znotif_prihod_pending|{record_id}"
                )
            ])
    except Exception as e:
        print("ZAPRAVSHIK PENDING PRIHOD LIST ERROR:", e)

    try:
        cursor.execute("""
            SELECT id, to_car, firm, liter, status, created_at
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND TRIM(COALESCE(status, '')) = 'Қабул қилувчи текширувида'
            ORDER BY created_at DESC
        """, (int(user_id),))
        for transfer_id, to_car, firm, liter, status, created_at in cursor.fetchall():
            date_text = created_at.strftime("%d.%m %H:%M") if created_at else ""
            buttons.append([
                InlineKeyboardButton(
                    f"➖ Расход | {date_text} | {liter} л | {to_car or '-'}"[:60],
                    callback_data=f"znotif_diesel_pending|{transfer_id}"
                )
            ])
    except Exception as e:
        print("ZAPRAVSHIK PENDING DIESEL LIST ERROR:", e)

    if not buttons:
        buttons.append([InlineKeyboardButton("✅ Текширувда турган дизел хабарлари йўқ", callback_data="none")])

    return InlineKeyboardMarkup(buttons)


def diesel_transfer_sender_card_text(row, status_text=None):
    if not row:
        return "❌ Маълумот топилмади."

    transfer_id, from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, video_id, status, receiver_comment, created_at = row
    created_text = created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else now_text()

    text = (
        "⛽ ДИЗЕЛ РАСХОД\n\n"
        f"🕒 Сана: {created_text}\n"
        f"🏢 Фирма: {firm or '-'}\n"
        f"🚛 Дизел берган техника: {from_car or '-'}\n"
        f"🚛 Дизел олган техника: {to_car or '-'}\n"
        f"⛽ Литр: {liter or '-'}\n"
        f"📝 Изоҳ: {note or '-'}\n"
    )

    if video_id:
        text += "🎥 Видео: сақланди ✅\n"

    text += f"📌 Статус: {status_text or status or '-'}\n"

    if receiver_comment:
        text += f"💬 Рад этиш изоҳи: {receiver_comment}\n"

    text += "\nМаълумот тўғрими?"
    return text


def get_diesel_transfer_full_row(transfer_id):
    cursor.execute("""
        SELECT id, from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, video_id, status, receiver_comment, created_at
        FROM diesel_transfers
        WHERE id = %s
        LIMIT 1
    """, (int(transfer_id),))
    return cursor.fetchone()


def diesel_rejected_sender_keyboard_conditional(transfer_id, has_media=True):
    rows = []
    if has_media:
        rows.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_rejected_view|{transfer_id}")])
    rows.append([InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_rejected_resend|{transfer_id}")])
    rows.append([InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_rejected_edit|{transfer_id}")])
    rows.append([InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_rejected_cancel|{transfer_id}")])
    return InlineKeyboardMarkup(rows)


def diesel_pending_sender_view_keyboard(transfer_id, has_media=True):
    rows = []
    if has_media:
        rows.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"znotif_diesel_pending_media|{transfer_id}")])
    return InlineKeyboardMarkup(rows)


def diesel_transfer_sender_after_view_keyboard(transfer_id, rejected=True):
    if rejected:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_rejected_resend|{transfer_id}")],
            [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_rejected_edit|{transfer_id}")],
            [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_rejected_cancel|{transfer_id}")],
        ])
    return InlineKeyboardMarkup([])



async def show_zapravshik_rejected_notifications_list(message, context, user_id):
    await clear_all_inline_messages(context, message.chat_id)
    context.user_data["mode"] = "zapravshik_diesel_notifications_rejected"
    await message.reply_text(
        "📩 Рад этилган приход ва расход хабарлари:",
        reply_markup=only_back_keyboard()
    )
    msg = await message.reply_text(
        "Рўйхатдан маълумотни танланг:",
        reply_markup=zapravshik_rejected_diesel_notifications_keyboard(user_id)
    )
    remember_inline_message(context, msg)


async def show_zapravshik_pending_notifications_list(message, context, user_id):
    await clear_all_inline_messages(context, message.chat_id)
    context.user_data["mode"] = "zapravshik_diesel_notifications_pending"
    await message.reply_text(
        "⏳ Текширувда турган приход ва расход хабарлари:",
        reply_markup=only_back_keyboard()
    )
    msg = await message.reply_text(
        "Рўйхатдан маълумотни танланг:",
        reply_markup=zapravshik_pending_diesel_notifications_keyboard(user_id)
    )
    remember_inline_message(context, msg)


async def send_zapravshik_prihod_returned_media(query, context, record_id):
    if not diesel_prihod_row_to_context(record_id, context):
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    if int(context.user_data.get("diesel_prihod_telegram_id") or 0) != int(query.from_user.id):
        await query.answer("Бу маълумот сиз учун эмас.", show_alert=True)
        return

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    # 1) Янги карточка
    await query.message.chat.send_message(
        diesel_prihod_card_text(
            context,
            status="Қайтди",
            receiver_comment=context.user_data.get("diesel_prihod_receiver_comment")
        )
    )

    # 2) Расм бўлса расм
    photo_id = context.user_data.get("diesel_prihod_photo_id")
    if photo_id:
        try:
            await query.message.chat.send_photo(photo=photo_id)
        except Exception as e:
            print("ZAPRAVSHIK RETURN PRIHOD PHOTO ERROR:", e)

    # 3) Видео бўлса видео
    video_id = context.user_data.get("diesel_prihod_video_id")
    if video_id:
        try:
            await query.message.chat.send_video_note(video_note=video_id)
        except Exception:
            try:
                await query.message.chat.send_video(video=video_id)
            except Exception as e:
                print("ZAPRAVSHIK RETURN PRIHOD VIDEO ERROR:", e)

    # 4) Энг пастда кнопкалар, Кўриш қайта чиқмайди
    msg = await query.message.chat.send_message(
        "Маълумот тўғрими?",
        reply_markup=diesel_prihod_sender_returned_after_view_keyboard(record_id)
    )
    remember_inline_message(context, msg)


async def send_zapravshik_prihod_notification_card(query, context, record_id, returned=True):
    context.user_data["mode"] = "zapravshik_notif_prihod_return_card" if returned else "zapravshik_notif_prihod_pending_card"
    context.user_data["zapravshik_notif_card_kind"] = "rejected" if returned else "pending"
    context.user_data["zapravshik_notif_record_id"] = str(record_id)

    if not diesel_prihod_row_to_context(record_id, context):
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    if int(context.user_data.get("diesel_prihod_telegram_id") or 0) != int(query.from_user.id):
        await query.answer("Бу маълумот сиз учун эмас.", show_alert=True)
        return

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    status = "Қайтди" if returned else "Текширувда"
    receiver_comment = context.user_data.get("diesel_prihod_receiver_comment") if returned else None

    if returned:
        reply_markup = diesel_prihod_sender_returned_keyboard(
            record_id,
            has_media=diesel_prihod_has_media(context)
        )
    else:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("👁 Кўриш", callback_data=f"znotif_prihod_pending_media|{record_id}")]
        ]) if diesel_prihod_has_media(context) else None

    msg = await query.message.chat.send_message(
        diesel_prihod_card_text(context, status=status, receiver_comment=receiver_comment),
        reply_markup=reply_markup
    )
    remember_inline_message(context, msg)


async def send_zapravshik_diesel_notification_card(query, context, transfer_id, rejected=True):
    context.user_data["mode"] = "zapravshik_notif_diesel_rejected_card" if rejected else "zapravshik_notif_diesel_pending_card"
    context.user_data["zapravshik_notif_card_kind"] = "rejected" if rejected else "pending"
    context.user_data["zapravshik_notif_transfer_id"] = str(transfer_id)

    row = get_diesel_transfer_full_row(transfer_id)
    if not row:
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    if int(row[1] or 0) != int(query.from_user.id):
        await query.answer("Бу маълумот сиз учун эмас.", show_alert=True)
        return

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    status_text = "Рад этилди" if rejected else "Қабул қилувчи текширувида"
    if rejected:
        reply_markup = diesel_rejected_sender_keyboard_conditional(transfer_id, has_media=bool(row[8]))
    else:
        reply_markup = diesel_pending_sender_view_keyboard(transfer_id, has_media=bool(row[8]))

    msg = await query.message.chat.send_message(
        diesel_transfer_sender_card_text(row, status_text=status_text),
        reply_markup=reply_markup
    )
    remember_inline_message(context, msg)


async def send_zapravshik_prihod_pending_media(query, context, record_id):
    if not diesel_prihod_row_to_context(record_id, context):
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    if int(context.user_data.get("diesel_prihod_telegram_id") or 0) != int(query.from_user.id):
        await query.answer("Бу маълумот сиз учун эмас.", show_alert=True)
        return

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    await query.message.chat.send_message(diesel_prihod_card_text(context, status="Текширувда"))

    photo_id = context.user_data.get("diesel_prihod_photo_id")
    if photo_id:
        try:
            await query.message.chat.send_photo(photo=photo_id)
        except Exception as e:
            print("ZNOTIF PRIHOD PENDING PHOTO ERROR:", e)

    video_id = context.user_data.get("diesel_prihod_video_id")
    if video_id:
        try:
            await query.message.chat.send_video_note(video_note=video_id)
        except Exception:
            try:
                await query.message.chat.send_video(video=video_id)
            except Exception as e:
                print("ZNOTIF PRIHOD PENDING VIDEO ERROR:", e)

def other_diesel_card_text(context, status="------"):
    return (
        "➖ БОШҚА ДИЗЕЛ РАСХОД\n\n"
        f"🕒 Сана: {context.user_data.get('other_diesel_time') or now_text()}\n"
        f"⛽ Литр: {context.user_data.get('other_diesel_liter', '')}\n"
        f"📝 Изоҳ: {context.user_data.get('other_diesel_note', '')}\n"
        "🎥 Видео: сақланди ✅\n"
        f"👤 Заправшик: {get_employee_full_name_by_telegram_id(context.user_data.get('other_diesel_telegram_id') or 0)}\n"
        f"📌 Статус: {status}"
    )


def other_diesel_confirm_keyboard(has_media=True):
    rows = []

    if has_media:
        rows.append([InlineKeyboardButton("👁 Кўриш", callback_data="other_diesel_view")])

    rows.append([
        InlineKeyboardButton("✅ Тасдиқлаш", callback_data="other_diesel_confirm"),
        InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="other_diesel_edit"),
    ])
    rows.append([InlineKeyboardButton("❌ Отмен", callback_data="other_diesel_cancel")])

    return InlineKeyboardMarkup(rows)


def other_diesel_after_view_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="other_diesel_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="other_diesel_edit"),
        ],
        [InlineKeyboardButton("❌ Отмен", callback_data="other_diesel_cancel")]
    ])


def diesel_prihod_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="diesel_prihod_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="diesel_prihod_edit"),
        ],
        [InlineKeyboardButton("❌ Отмен", callback_data="diesel_prihod_cancel")]
    ])


def diesel_prihod_sender_returned_keyboard(record_id, has_media=True):
    rows = []

    if has_media:
        rows.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_prihod_return_media|{record_id}")])

    rows.append([
        InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_prihod_resend|{record_id}"),
        InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_prihod_return_edit|{record_id}"),
    ])
    rows.append([InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_prihod_return_cancel|{record_id}")])

    return InlineKeyboardMarkup(rows)


def diesel_prihod_technadzor_keyboard(record_id, has_media=True):
    rows = []

    if has_media:
        rows.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_prihod_media|{record_id}")])

    rows.append([
        InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_prihod_approve|{record_id}"),
        InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_prihod_tech_edit|{record_id}"),
    ])
    rows.append([InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_prihod_reject|{record_id}")])

    return InlineKeyboardMarkup(rows)


def diesel_prihod_has_media(context):
    return bool(
        context.user_data.get("diesel_prihod_video_id")
        or context.user_data.get("diesel_prihod_photo_id")
    )


def diesel_prihod_technadzor_after_view_keyboard(record_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_prihod_approve|{record_id}"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_prihod_tech_edit|{record_id}"),
        ],
        [InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_prihod_reject|{record_id}")]
    ])


def diesel_prihod_sender_returned_after_view_keyboard(record_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_prihod_resend|{record_id}"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_prihod_return_edit|{record_id}"),
        ],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_prihod_return_cancel|{record_id}")]
    ])


def diesel_prihod_edit_keyboard(prefix="diesel_prihod_edit"):
    def cb(field):
        if "|" in prefix:
            return f"{prefix}|{field}"
        return f"{prefix}_{field}"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏢 Фирма", callback_data=cb("firm"))],
        [InlineKeyboardButton("⛽ Литр", callback_data=cb("liter"))],
        [InlineKeyboardButton("📝 Изоҳ", callback_data=cb("note"))],
        [InlineKeyboardButton("🎥 Видео", callback_data=cb("video"))],
        [InlineKeyboardButton("🖼 Расм", callback_data=cb("photo"))],
    ])


def is_valid_liter_amount(value):
    value = (value or "").strip()
    return value.isdigit() and int(value) > 0


def is_valid_text_number_note(value):
    value = (value or "").strip()
    if len(value) < 1 or len(value) > 200:
        return False
    return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳІіЇїЄє0-9\s\-_/.,#№]+", value))


def get_employee_full_name_by_telegram_id(telegram_id):
    try:
        cursor.execute("""
            SELECT name, surname
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(telegram_id),))
        row = cursor.fetchone()
        if row:
            name = row[0] or ""
            surname = row[1] or ""
            full = f"{surname} {name}".strip()
            if full:
                return full
    except Exception as e:
        print("GET EMPLOYEE FULL NAME ERROR:", e)

    user = USERS.get(int(telegram_id)) if str(telegram_id).isdigit() else None
    return user.get("name") if user else "Номаълум"



DIESEL_PRIHOD_FIRM_PREFIX = "Фирма: "


def encode_diesel_prihod_note(firm, note):
    firm = (firm or "").strip()
    note = (note or "").strip()
    return f"{DIESEL_PRIHOD_FIRM_PREFIX}{firm}\n{note}"


def parse_diesel_prihod_note(raw_note):
    raw_note = raw_note or ""
    receiver_comment = ""

    if "\nРад изоҳи: " in raw_note:
        raw_note, receiver_comment = raw_note.split("\nРад изоҳи: ", 1)

    firm = ""
    note = raw_note

    if raw_note.startswith(DIESEL_PRIHOD_FIRM_PREFIX):
        lines = raw_note.split("\n", 1)
        firm = lines[0].replace(DIESEL_PRIHOD_FIRM_PREFIX, "", 1).strip()
        note = lines[1].strip() if len(lines) > 1 else ""

    return firm, note, receiver_comment



# ================= V26 ZAPRAVSHIK ONLY REPORTS + STOCK FILTER =================
# Dependency-free .xlsx generator: openpyxl талаб қилмайди, Render requirements бузилмайди.
def _xlsx_col_name(col_index):
    name = ""
    while col_index:
        col_index, rem = divmod(col_index - 1, 26)
        name = chr(65 + rem) + name
    return name


def _xlsx_sheet_xml(rows):
    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, value in enumerate(row, start=1):
            ref = f"{_xlsx_col_name(c_idx)}{r_idx}"
            text = "" if value is None else str(value)
            cells.append(
                f'<c r="{ref}" t="inlineStr"><is><t>{escape(text)}</t></is></c>'
            )
        sheet_rows.append(f'<row r="{r_idx}">{"".join(cells)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheetData>' + ''.join(sheet_rows) + '</sheetData>'
        '</worksheet>'
    )


def build_xlsx_file(sheets):
    """sheets = [(sheet_name, rows), ...] -> BytesIO"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>""")
        z.writestr("_rels/.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""")
        z.writestr("xl/styles.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts><fills count="1"><fill><patternFill patternType="none"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs></styleSheet>""")

        workbook_sheets = []
        rels = []
        for idx, (sheet_name, rows) in enumerate(sheets, start=1):
            safe_name = escape(str(sheet_name)[:31])
            workbook_sheets.append(f'<sheet name="{safe_name}" sheetId="{idx}" r:id="rId{idx}"/>')
            rels.append(
                f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
            )
            z.writestr(f"xl/worksheets/sheet{idx}.xml", _xlsx_sheet_xml(rows))

        z.writestr("xl/workbook.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>""" + ''.join(workbook_sheets) + "</sheets></workbook>")
        z.writestr("xl/_rels/workbook.xml.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">""" + ''.join(rels) + "</Relationships>")

    buffer.seek(0)
    return buffer


def _report_full_name(name, surname, fallback=""):
    full = f"{surname or ''} {name or ''}".strip()
    return full or fallback or "-"


def _report_approved_name(approved_by_id=None, approved_by_name=None, name=None, surname=None, fallback=""):
    saved_name = (approved_by_name or "").strip()
    if saved_name:
        return saved_name

    joined_name = f"{surname or ''} {name or ''}".strip()
    if joined_name:
        return joined_name

    if approved_by_id:
        resolved = get_employee_full_name_by_telegram_id(approved_by_id)
        if resolved and resolved != "Номаълум":
            return resolved

    return fallback or ""


def _report_date(value):
    try:
        return value.strftime("%d.%m.%Y %H:%M") if value else ""
    except Exception:
        return ""


def build_zapravshik_diesel_report_file():
    rashod_headers = [
        "Сана",
        "Фирма номи",
        "Литр",
        "Спидометр/моточас",
        "Ким томонидан берилган",
        "Ким томонидан олинган",
        "Ким тасдиқлаган",
        "Статус",
    ]

    # Дизел приход листида "Спидометр/моточас" устуни керак эмас.
    # Расход листи структурасига тегилмайди.
    prihod_headers = [
        "Сана",
        "Фирма номи",
        "Литр",
        "Ким томонидан берилган",
        "Ким томонидан олинган",
        "Ким тасдиқлаган",
        "Статус",
    ]

    rashod_rows = [rashod_headers]
    prihod_rows = [prihod_headers]

    cursor.execute("""
        SELECT
            dt.created_at,
            dt.firm,
            dt.liter,
            dt.speedometer_photo_id,
            dt.from_driver_id,
            dt.from_car,
            fd.name,
            fd.surname,
            dt.to_driver_id,
            dt.to_car,
            td.name,
            td.surname,
            dt.approved_by_id,
            dt.approved_by_name,
            ab.name,
            ab.surname,
            dt.status
        FROM diesel_transfers dt
        LEFT JOIN drivers fd ON fd.telegram_id = dt.from_driver_id
        LEFT JOIN drivers td ON td.telegram_id = dt.to_driver_id
        LEFT JOIN drivers ab ON ab.telegram_id = dt.approved_by_id
        WHERE TRIM(COALESCE(dt.from_car, '')) = 'Заправщик'
        ORDER BY dt.created_at DESC, dt.id DESC
    """)

    for row in cursor.fetchall():
        (
            created_at, firm, liter, speedometer_value,
            from_driver_id, from_car, fd_name, fd_surname,
            to_driver_id, to_car, td_name, td_surname,
            approved_by_id, approved_by_name, ab_name, ab_surname,
            status,
        ) = row

        if str(from_car or "").strip() == "Заправщик":
            giver = _report_full_name(fd_name, fd_surname, "Заправщик")
        else:
            giver = _report_full_name(fd_name, fd_surname, str(from_car or "-"))

        receiver = _report_full_name(td_name, td_surname, str(to_car or "-"))

        approved_by = _report_approved_name(
            approved_by_id=approved_by_id,
            approved_by_name=approved_by_name,
            name=ab_name,
            surname=ab_surname,
            fallback="Автоматик" if (status or "").strip() == "Тасдиқланди" and not to_driver_id else ""
        )

        rashod_rows.append([
            _report_date(created_at),
            firm or "",
            liter or "",
            speedometer_value or "",
            giver,
            receiver,
            approved_by,
            diesel_status_display(status),
        ])

    cursor.execute("""
        SELECT
            dp.created_at,
            dp.liter,
            dp.note,
            dp.telegram_id,
            d.name,
            d.surname,
            dp.approved_by_id,
            dp.approved_by_name,
            ab.name,
            ab.surname,
            dp.status
        FROM diesel_prihod dp
        LEFT JOIN drivers d ON d.telegram_id = dp.telegram_id
        LEFT JOIN drivers ab ON ab.telegram_id = dp.approved_by_id
        ORDER BY dp.created_at DESC, dp.id DESC
    """)

    for row in cursor.fetchall():
        created_at, liter, note, telegram_id, name, surname, approved_by_id, approved_by_name, ab_name, ab_surname, status = row
        firm, _, _ = parse_diesel_prihod_note(note or "")
        giver = _report_full_name(name, surname, get_employee_full_name_by_telegram_id(telegram_id) if telegram_id else "-")
        approved_by = _report_approved_name(
            approved_by_id=approved_by_id,
            approved_by_name=approved_by_name,
            name=ab_name,
            surname=ab_surname,
            fallback="Тасдиқловчи сақланмаган" if (status or "").strip() == "Тасдиқланди" else ""
        )

        prihod_rows.append([
            _report_date(created_at),
            firm or "",
            liter or "",
            giver,
            "Заправка омбори",
            approved_by,
            status or "",
        ])

    return build_xlsx_file([
        ("Дизел Расходлар", rashod_rows),
        ("Дизел Приходлар", prihod_rows),
    ])


def _report_person_name_for_excel(value):
    raw = "" if value is None else str(value).strip()
    if not raw:
        return ""

    numeric = raw.replace("+", "", 1)
    if numeric.isdigit():
        resolved = get_employee_full_name_by_telegram_id(raw)
        if resolved and resolved != "Номаълум":
            return resolved

    return raw



def _technadzor_diesel_summary_rows():
    rows = [["⛽ ЗАПРАВКА МАЬЛУМОТИ"], []]
    total_prihod = 0.0
    total_rashod = 0.0
    total_ostatka = 0.0
    stock_data = get_all_diesel_stock_by_firm_cached()

    for firm in FIRM_NAMES:
        item = stock_data.get(firm, {})
        prihod = float(item.get("prihod", 0) or 0)
        rashod = float(item.get("rashod", 0) or 0)
        ostatka = prihod - rashod
        total_prihod += prihod
        total_rashod += rashod
        total_ostatka += ostatka

        rows.append([firm])
        rows.append([f"Приход: {format_liter(prihod)}л / Расход: {format_liter(rashod)}л / Остатка: {format_liter(ostatka)}л"])
        rows.append([])

    other_total = get_other_diesel_expense_total()
    total_rashod += float(other_total or 0)
    total_ostatka -= float(other_total or 0)

    rows.append(["📦 Бошқа дизел расходлар"])
    rows.append([f"Остатка: -{format_liter(other_total)}л"])
    rows.append(["--------------------------------"])
    rows.append(["✅ ИТОГО:"])
    rows.append([f"Приход: {format_liter(total_prihod)}л / Расход: {format_liter(total_rashod)}л / Остатка: {format_liter(total_ostatka)}л"])
    return rows


def build_technadzor_diesel_report_file():
    common_headers = [
        "Сана",
        "Фирма номи",
        "Техника",
        "Литр",
        "Спидометр/моточас",
        "Ким томонидан берилган",
        "Ким томонидан олинган",
        "Ким тасдиқлаган",
        "Статус",
        "Изоҳ",
    ]

    zap_prihod_rows = [[
        "Сана",
        "Фирма номи",
        "Литр",
        "Ким приход қилган",
        "Ким тасдиқлаган",
        "Статус",
        "Изоҳ",
    ]]
    zap_rashod_rows = [common_headers]
    driver_rows = [["Тури"] + common_headers]

    # 2-лист: Заправщик приход қилган дизел маълумотлари.
    cursor.execute("""
        SELECT
            dp.created_at,
            dp.liter,
            dp.note,
            dp.telegram_id,
            d.name,
            d.surname,
            dp.approved_by_id,
            dp.approved_by_name,
            ab.name,
            ab.surname,
            dp.status
        FROM diesel_prihod dp
        LEFT JOIN drivers d ON d.telegram_id = dp.telegram_id
        LEFT JOIN drivers ab ON ab.telegram_id = dp.approved_by_id
        WHERE COALESCE(d.work_role, 'driver') = 'zapravshik'
        ORDER BY dp.created_at DESC, dp.id DESC
    """)
    for row in cursor.fetchall():
        created_at, liter, note, telegram_id, name, surname, approved_by_id, approved_by_name, ab_name, ab_surname, status = row
        firm, note_text, receiver_comment = parse_diesel_prihod_note(note or "")
        giver = _report_full_name(name, surname, get_employee_full_name_by_telegram_id(telegram_id) if telegram_id else "-")
        approved_by = _report_approved_name(
            approved_by_id=approved_by_id,
            approved_by_name=approved_by_name,
            name=ab_name,
            surname=ab_surname,
            fallback="Тасдиқловчи сақланмаган" if (status or "").strip() == "Тасдиқланди" else ""
        )
        zap_prihod_rows.append([
            _report_date(created_at),
            firm or "",
            liter or "",
            giver,
            approved_by,
            diesel_status_display(status),
            note_text or "",
        ])

    # 3-лист: Заправщик расход қилган дизел маълумотлари.
    cursor.execute("""
        SELECT
            dt.created_at,
            dt.firm,
            dt.to_car,
            dt.liter,
            dt.speedometer_photo_id,
            dt.from_driver_id,
            fd.name,
            fd.surname,
            dt.to_driver_id,
            td.name,
            td.surname,
            dt.approved_by_id,
            dt.approved_by_name,
            ab.name,
            ab.surname,
            dt.status,
            dt.note
        FROM diesel_transfers dt
        LEFT JOIN drivers fd ON fd.telegram_id = dt.from_driver_id
        LEFT JOIN drivers td ON td.telegram_id = dt.to_driver_id
        LEFT JOIN drivers ab ON ab.telegram_id = dt.approved_by_id
        WHERE TRIM(COALESCE(dt.from_car, '')) = 'Заправщик'
        ORDER BY dt.created_at DESC, dt.id DESC
    """)
    for row in cursor.fetchall():
        (
            created_at, firm, to_car, liter, speedometer_value,
            from_driver_id, fd_name, fd_surname,
            to_driver_id, td_name, td_surname,
            approved_by_id, approved_by_name, ab_name, ab_surname,
            status, note,
        ) = row
        giver = _report_full_name(fd_name, fd_surname, "Заправщик")
        receiver = _report_full_name(td_name, td_surname, str(to_car or "-"))
        approved_by = _report_approved_name(
            approved_by_id=approved_by_id,
            approved_by_name=approved_by_name,
            name=ab_name,
            surname=ab_surname,
            fallback="Автоматик" if (status or "").strip() == "Тасдиқланди" and not to_driver_id else ""
        )
        zap_rashod_rows.append([
            _report_date(created_at),
            firm or "",
            to_car or "",
            liter or "",
            speedometer_value or "",
            giver,
            receiver,
            approved_by,
            diesel_status_display(status),
            note or "",
        ])

    # 4-лист: Ҳайдовчилар приход ва расход қилган дизел маълумотлари.
    cursor.execute("""
        SELECT
            dt.created_at,
            dt.firm,
            dt.from_car,
            dt.to_car,
            dt.liter,
            dt.speedometer_photo_id,
            dt.from_driver_id,
            fd.name,
            fd.surname,
            COALESCE(fd.work_role, 'driver') AS from_role,
            dt.to_driver_id,
            td.name,
            td.surname,
            COALESCE(td.work_role, 'driver') AS to_role,
            dt.approved_by_id,
            dt.approved_by_name,
            ab.name,
            ab.surname,
            dt.status,
            dt.note
        FROM diesel_transfers dt
        LEFT JOIN drivers fd ON fd.telegram_id = dt.from_driver_id
        LEFT JOIN drivers td ON td.telegram_id = dt.to_driver_id
        LEFT JOIN drivers ab ON ab.telegram_id = dt.approved_by_id
        WHERE TRIM(COALESCE(dt.from_car, '')) <> 'Заправщик'
           OR COALESCE(td.work_role, 'driver') = 'driver'
        ORDER BY dt.created_at DESC, dt.id DESC
    """)
    for row in cursor.fetchall():
        (
            created_at, firm, from_car, to_car, liter, speedometer_value,
            from_driver_id, fd_name, fd_surname, from_role,
            to_driver_id, td_name, td_surname, to_role,
            approved_by_id, approved_by_name, ab_name, ab_surname,
            status, note,
        ) = row

        giver = _report_full_name(fd_name, fd_surname, str(from_car or "-"))
        receiver = _report_full_name(td_name, td_surname, str(to_car or "-"))
        approved_by = _report_approved_name(
            approved_by_id=approved_by_id,
            approved_by_name=approved_by_name,
            name=ab_name,
            surname=ab_surname,
            fallback="Автоматик" if (status or "").strip() == "Тасдиқланди" and not to_driver_id else ""
        )
        if (from_role or "driver") == "driver" and (to_role or "driver") == "driver":
            report_type = "Ҳайдовчи расход / Ҳайдовчи приход"
        elif (from_role or "driver") == "driver":
            report_type = "Ҳайдовчи расход"
        else:
            report_type = "Ҳайдовчи приход"

        driver_rows.append([
            report_type,
            _report_date(created_at),
            firm or "",
            to_car or from_car or "",
            liter or "",
            speedometer_value or "",
            giver,
            receiver,
            approved_by,
            diesel_status_display(status),
            note or "",
        ])

    return build_xlsx_file([
        ("Заправка маълумоти", _technadzor_diesel_summary_rows()),
        ("Приход ДИЗЕЛ заправшика", zap_prihod_rows),
        ("Расход ДИЗЕЛ заправшика", zap_rashod_rows),
        ("Хайдовчилар приход расходи", driver_rows),
    ])


def build_technadzor_remont_report_file():
    headers = [
        "ID",
        "Фирма",
        "Техника номери",
        "Техника тури",
        "КМ/Моточас",
        "Ремонт тури",
        "Статус",
        "Изоҳ",
        "Механик/Киритган",
        "Ремонтдан чиқарган",
        "Текширувчи",
        "Ремонтга кирган сана",
        "Ремонтдан чиққан сана",
        "Тасдиқланган сана",
    ]

    rows = [headers]

    cursor.execute("""
        SELECT
            r.id,
            COALESCE(c.firm, ''),
            r.car_number,
            COALESCE(c.car_type, ''),
            r.km,
            r.repair_type,
            r.status,
            r.comment,
            r.entered_by,
            r.exited_by,
            r.approved_by,
            r.entered_at,
            r.exited_at,
            r.approved_at
        FROM repairs r
        LEFT JOIN cars c ON LOWER(TRIM(c.car_number)) = LOWER(TRIM(r.car_number))
        ORDER BY COALESCE(r.approved_at, r.exited_at, r.entered_at) DESC, r.id DESC
    """)

    for row in cursor.fetchall():
        (
            repair_id,
            firm,
            car_number,
            car_type,
            km,
            repair_type,
            status,
            comment,
            entered_by,
            exited_by,
            approved_by,
            entered_at,
            exited_at,
            approved_at,
        ) = row

        rows.append([
            repair_id or "",
            firm or "",
            car_number or "",
            car_type or "",
            km or "",
            repair_type or "",
            status or "",
            comment or "",
            _report_person_name_for_excel(entered_by),
            _report_person_name_for_excel(exited_by),
            _report_person_name_for_excel(approved_by),
            _report_date(entered_at),
            _report_date(exited_at),
            _report_date(approved_at),
        ])

    return build_xlsx_file([
        ("Отчет Ремонт", rows),
    ])


def get_diesel_prihod_note_for_db(context):
    return encode_diesel_prihod_note(
        context.user_data.get("diesel_prihod_firm", ""),
        context.user_data.get("diesel_prihod_note", "")
    )


def diesel_prihod_card_text(context, status="Текширувда", receiver_comment=None):
    created_at = context.user_data.get("diesel_prihod_time") or now_text()
    accepted_by = get_employee_full_name_by_telegram_id(
        context.user_data.get("diesel_prihod_telegram_id")
        or context.user_data.get("telegram_id")
        or 0
    )

    text = (
        "✅ ДИЗЕЛ ПРИХОД\n\n"
        f"🕒 Сана: {created_at}\n"
        f"🏢 Фирма: {context.user_data.get('diesel_prihod_firm', '-') or '-'}\n"
        f"⛽ Литр: {context.user_data.get('diesel_prihod_liter', '')}\n"
        f"📝 Изоҳ: {context.user_data.get('diesel_prihod_note', '')}\n"
    )

    if context.user_data.get("diesel_prihod_video_id"):
        text += "🎥 Видео: сақланди ✅\n"

    if context.user_data.get("diesel_prihod_photo_id"):
        text += "🖼 Расм: сақланди ✅\n"

    text += (
        f"👤 Қабул қилди: {accepted_by}\n"
        f"📌 Статус: {status}\n"
    )

    if receiver_comment:
        text += f"💬 Рад этиш изоҳи: {receiver_comment}\n"

    text += "\nМаълумот тўғрими?"
    return text


def diesel_prihod_row_to_context(record_id, context):
    cursor.execute("""
        SELECT id, telegram_id, liter, note, video_id, photo_id, status, created_at
        FROM diesel_prihod
        WHERE id = %s
        LIMIT 1
    """, (int(record_id),))
    row = cursor.fetchone()

    if not row:
        return None

    firm_text, note_text, receiver_comment = parse_diesel_prihod_note(row[3] or "")

    context.user_data["diesel_prihod_id"] = row[0]
    context.user_data["diesel_prihod_telegram_id"] = row[1]
    context.user_data["diesel_prihod_firm"] = firm_text or "Фирма танланмаган"
    context.user_data["diesel_prihod_liter"] = str(row[2])
    context.user_data["diesel_prihod_note"] = note_text
    context.user_data["diesel_prihod_video_id"] = row[4]
    context.user_data["diesel_prihod_photo_id"] = row[5]
    context.user_data["diesel_prihod_status"] = row[6] or ""
    context.user_data["diesel_prihod_receiver_comment"] = receiver_comment
    context.user_data["diesel_prihod_time"] = row[7].strftime("%Y-%m-%d %H:%M:%S") if row[7] else now_text()

    return row


async def send_diesel_prihod_to_technadzor(context, record_id):
    row = diesel_prihod_row_to_context(record_id, context)
    if not row:
        return

    # Асосий экранда фақат огоҳлантириш боради.
    # Карточка ва тасдиқ/таҳрир/рад этиш фақат:
    # 🔔 Уведомления → ⛽ Дизел приход менюси ичида очилади.
    for tech_id in get_user_ids_by_role("technadzor"):
        try:
            await context.bot.send_message(
                chat_id=int(tech_id),
                text="🔔 Уведомления\n⛽ Дизел приход"
            )
        except Exception as e:
            print("SEND DIESEL PRIHOD TO TECHNADZOR ERROR:", e)


async def send_diesel_prihod_returned_to_sender(context, record_id, reason):
    row = diesel_prihod_row_to_context(record_id, context)
    if not row:
        return

    sender_id = context.user_data.get("diesel_prihod_telegram_id")
    card_text = diesel_prihod_card_text(context, status="Қайтди", receiver_comment=reason)

    try:
        await context.bot.send_message(
            chat_id=int(sender_id),
            text=card_text,
            reply_markup=diesel_prihod_sender_returned_keyboard(record_id, has_media=diesel_prihod_has_media(context))
        )
    except Exception as e:
        print("SEND DIESEL PRIHOD RETURNED ERROR:", e)




def diesel_prihod_mark_staged(context, record_id):
    context.user_data["diesel_prihod_staged_edit"] = True
    context.user_data["diesel_prihod_staged_record_id"] = str(record_id)


def diesel_prihod_clear_staged(context):
    for key in [
        "diesel_prihod_staged_edit",
        "diesel_prihod_staged_record_id",
    ]:
        context.user_data.pop(key, None)


def diesel_prihod_has_staged_for(context, record_id):
    return (
        bool(context.user_data.get("diesel_prihod_staged_edit"))
        and str(context.user_data.get("diesel_prihod_staged_record_id")) == str(record_id)
    )


async def apply_diesel_prihod_staged_edits(context, record_id):
    if not diesel_prihod_has_staged_for(context, record_id):
        return

    cursor.execute("""
        UPDATE diesel_prihod
        SET liter = %s,
            note = %s,
            video_id = %s,
            photo_id = %s
        WHERE id = %s
    """, (
        context.user_data.get("diesel_prihod_liter"),
        get_diesel_prihod_note_for_db(context),
        context.user_data.get("diesel_prihod_video_id"),
        context.user_data.get("diesel_prihod_photo_id"),
        int(record_id)
    ))
    conn.commit()
    diesel_prihod_clear_staged(context)


async def show_diesel_prihod_staged_card(message, context, record_id):
    source = context.user_data.get("diesel_prihod_edit_source")
    status = context.user_data.get("diesel_prihod_status") or "Текширувда"

    if source == "returned":
        reply_markup = diesel_prihod_sender_returned_keyboard(record_id)
    else:
        reply_markup = diesel_prihod_technadzor_keyboard(record_id, has_media=diesel_prihod_has_media(context))

    await message.reply_text(
        diesel_prihod_card_text(
            context,
            status=status,
            receiver_comment=context.user_data.get("diesel_prihod_receiver_comment")
        ),
        reply_markup=reply_markup
    )


async def show_diesel_prihod_db_card_after_edit(message, context, record_id):
    if diesel_prihod_has_staged_for(context, record_id):
        await show_diesel_prihod_staged_card(message, context, record_id)
        return

    row = diesel_prihod_row_to_context(record_id, context)
    if not row:
        await message.reply_text("❌ Маълумот топилмади.", reply_markup=zapravshik_diesel_menu_keyboard())
        return

    source = context.user_data.get("diesel_prihod_edit_source")
    status = context.user_data.get("diesel_prihod_status") or "Текширувда"

    if source == "returned":
        reply_markup = diesel_prihod_sender_returned_keyboard(record_id)
    else:
        reply_markup = diesel_prihod_technadzor_keyboard(record_id, has_media=diesel_prihod_has_media(context))

    await message.reply_text(
        diesel_prihod_card_text(context, status=status, receiver_comment=context.user_data.get("diesel_prihod_receiver_comment")),
        reply_markup=reply_markup
    )



def diesel_prihod_pending_keyboard():
    try:
        cursor.execute("""
            SELECT id, telegram_id, liter, note, status, created_at
            FROM diesel_prihod
            WHERE TRIM(COALESCE(status, '')) = 'Текширувда'
            ORDER BY created_at ASC
        """)
        rows = cursor.fetchall()
    except Exception as e:
        print("DIESEL PRIHOD PENDING LIST ERROR:", e)
        rows = []

    if not rows:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Текширувда дизел приход йўқ", callback_data="none")]])

    buttons = []
    for record_id, telegram_id, liter, note, status, created_at in rows:
        date_text = created_at.strftime("%d.%m %H:%M") if created_at else ""
        firm_text, _, _ = parse_diesel_prihod_note(note or "")
        firm_text = firm_text or "Фирма танланмаган"
        buttons.append([
            InlineKeyboardButton(
                f"{date_text} | {liter} л | {firm_text}"[:60],
                callback_data=f"diesel_prihod_view|{record_id}"
            )
        ])

    return InlineKeyboardMarkup(buttons)


async def open_diesel_prihod_for_technadzor(query, context, record_id):
    if not diesel_prihod_row_to_context(record_id, context):
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    context.user_data["mode"] = "technadzor_diesel_prihod_card"
    context.user_data["diesel_prihod_current_id"] = str(record_id)

    card_text = diesel_prihod_card_text(
        context,
        status=context.user_data.get("diesel_prihod_status", "Текширувда"),
        receiver_comment=context.user_data.get("diesel_prihod_receiver_comment")
    )

    # Эски рўйхат хабарини ўчирамиз. Ўчмаса, камида inline кнопкаларини олиб ташлаймиз.
    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    card_msg = await query.message.chat.send_message(
        card_text,
        reply_markup=diesel_prihod_technadzor_keyboard(record_id, has_media=diesel_prihod_has_media(context))
    )
    remember_inline_message(context, card_msg)


async def send_diesel_prihod_media_for_technadzor(query, context, record_id):
    if not diesel_prihod_row_to_context(record_id, context):
        await query.answer("Маълумот топилмади.", show_alert=True)
        return

    context.user_data["mode"] = "technadzor_diesel_prihod_card"
    context.user_data["diesel_prihod_current_id"] = str(record_id)

    try:
        await clear_all_inline_messages(context, query.message.chat_id)
    except Exception:
        pass

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    # 1) Янги карточка
    await query.message.chat.send_message(
        diesel_prihod_card_text(
            context,
            status=context.user_data.get("diesel_prihod_status", "Текширувда"),
            receiver_comment=context.user_data.get("diesel_prihod_receiver_comment")
        )
    )

    # 2) Расм бўлса расм
    photo_id = context.user_data.get("diesel_prihod_photo_id")
    if photo_id:
        try:
            await query.message.chat.send_photo(photo=photo_id)
        except Exception as e:
            print("DIESEL PRIHOD MEDIA PHOTO SEND ERROR:", e)

    # 3) Видео бўлса видео
    video_id = context.user_data.get("diesel_prihod_video_id")
    if video_id:
        try:
            await query.message.chat.send_video_note(video_note=video_id)
        except Exception:
            try:
                await query.message.chat.send_video(video=video_id)
            except Exception as e:
                print("DIESEL PRIHOD MEDIA VIDEO SEND ERROR:", e)

    # 4) Энг пастда кнопкалар, Кўриш қайта чиқмайди
    card_msg = await query.message.chat.send_message(
        "Маълумот тўғрими?",
        reply_markup=diesel_prihod_technadzor_after_view_keyboard(record_id)
    )
    remember_inline_message(context, card_msg)


def diesel_get_type_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⛽ Заправкадан")],
        [KeyboardButton("🚛 Техникадан")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)

def gas_firm_keyboard():
    cursor.execute("""
        SELECT DISTINCT firm
        FROM cars
        WHERE LOWER(fuel_type) = LOWER('Газ')
          AND firm IS NOT NULL
          AND firm <> ''
        ORDER BY firm
    """)
    rows = cursor.fetchall()

    buttons = [[KeyboardButton(row[0])] for row in rows]
    buttons.append([KeyboardButton("⬅️ Орқага")])

    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def gas_cars_by_firm_keyboard(firm, exclude_car=None, callback_prefix="gasgive_car"):
    cursor.execute("""
        SELECT car_number, car_type
        FROM cars
        WHERE LOWER(firm) = LOWER(%s)
          AND LOWER(fuel_type) = LOWER('Газ')
          AND (%s IS NULL OR LOWER(car_number) <> LOWER(%s))
        ORDER BY car_number
    """, (firm, exclude_car, exclude_car))

    rows = cursor.fetchall()
    keyboard = []

    for car_number, car_type in rows:
        keyboard.append([
            InlineKeyboardButton(
                f"{car_number} | {car_type}",
                callback_data=f"{callback_prefix}|{car_number}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Газли техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)

def diesel_firm_keyboard():
    cursor.execute("""
        SELECT DISTINCT firm
        FROM cars
        WHERE LOWER(fuel_type) = LOWER('Дизел')
          AND firm IS NOT NULL
          AND firm <> ''
        ORDER BY firm
    """)
    rows = cursor.fetchall()

    buttons = [[KeyboardButton(row[0])] for row in rows]
    buttons.append([KeyboardButton("⬅️ Орқага")])

    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def diesel_cars_by_firm_keyboard(firm, exclude_car=None):
    cursor.execute("""
        SELECT car_number, car_type
        FROM cars
        WHERE LOWER(firm) = LOWER(%s)
          AND LOWER(fuel_type) = LOWER('Дизел')
          AND (%s IS NULL OR LOWER(car_number) <> LOWER(%s))
        ORDER BY car_number
    """, (firm, exclude_car, exclude_car))

    rows = cursor.fetchall()
    keyboard = []

    for car_number, car_type in rows:
        keyboard.append([
            InlineKeyboardButton(
                f"{car_number} | {car_type}",
                callback_data=f"dieselgive_car|{car_number}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Дизел техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)


def diesel_pending_confirm_keyboard(driver_car):
    cursor.execute("""
        SELECT
            dt.id,
            dt.from_car,
            dt.firm,
            dt.created_at,
            COALESCE(dt.liter, '')
        FROM diesel_transfers dt
        WHERE LOWER(dt.to_car) = LOWER(%s)
          AND dt.status = 'Қабул қилувчи текширувида'
        ORDER BY dt.created_at DESC
    """, (driver_car,))

    rows = cursor.fetchall()
    keyboard = []

    for transfer_id, from_car, firm, created_at, liter in rows:
        from_car = from_car or "Кимдан номаълум"
        date_text = created_at.strftime("%d.%m") if created_at else "--.--"
        liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter)

        keyboard.append([
            InlineKeyboardButton(
                f"{date_text} / {from_car} / {liter_text} л",
                callback_data=f"diesel_receive_detail|{transfer_id}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Тасдиқлашда турган дизел маълумоти йўқ", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)


def diesel_receive_action_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_receive_reject|{transfer_id}")
        ]
    ])


def gas_give_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="gasgive_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="gasgive_edit")
        ],
        [InlineKeyboardButton("❌ Отмен", callback_data="gasgive_cancel")]
    ])


def diesel_give_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="dieselgive_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="dieselgive_edit")
        ]
    ])

def diesel_give_final_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="dieselgive_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="dieselgive_edit")
        ],
        [InlineKeyboardButton("❌ Отмен", callback_data="dieselgive_cancel")]
    ])


def diesel_give_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚛 Техникани ўзгартириш", callback_data="diesel_edit_car")],
        [InlineKeyboardButton("⛽ Литр", callback_data="diesel_edit_liter")],
        [InlineKeyboardButton("📝 Изоҳ", callback_data="diesel_edit_note")],
        [InlineKeyboardButton("📍 Спидометр/моточас кўрсаткичи", callback_data="diesel_edit_speed_photo")],
        [InlineKeyboardButton("🎥 Видео", callback_data="diesel_edit_video")]
    ])


def gas_give_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚛 Газ олган техникани ўзгартириш", callback_data="gasgive_edit_to_car")],
        [InlineKeyboardButton("📝 Изоҳ", callback_data="gasgive_edit_note")],
        [InlineKeyboardButton("🎥 Видео", callback_data="gasgive_edit_video")]
    ])


def gas_receiver_confirm_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"gas_receive_view|{transfer_id}")],
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"gasgive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"gasgive_reject|{transfer_id}")
        ]
    ])


def gas_receiver_after_view_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"gasgive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"gasgive_reject|{transfer_id}")
        ]
    ])


def gas_rejected_sender_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"gas_rejected_view|{transfer_id}")],
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"gas_rejected_resend|{transfer_id}")],
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"gas_rejected_edit|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"gas_rejected_cancel|{transfer_id}")]
    ])


def gas_rejected_after_view_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"gas_rejected_resend|{transfer_id}")],
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"gas_rejected_edit|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"gas_rejected_cancel|{transfer_id}")]
    ])

def view_media_keyboard(media_key):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"view_media|{media_key}")]
    ])


def is_valid_gas_note(text):
    return bool(re.match(r"^[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳ0-9\s]+$", text.strip())) and len(text.strip()) >= 2


def is_valid_diesel_liter(text):
    return text.isdigit() and 1 <= int(text) <= 9999


def diesel_status_display(status):
    # DB statuslarini o‘zgartirmaymiz. Faqat foydalanuvchiga ko‘rinadigan nomni chiqaramiz.
    if status == "Қабул қилувчи текширувида":
        return "Қабул қилувчида"
    if status == "Тасдиқланди":
        return "Қабул қилинди"
    if status == "Рад этилди":
        return "Рад этилди"
    return status or "Номаълум"


def diesel_confirm_text(context):
    created_time = context.user_data.get("dieselgive_created_time") or now_text()
    context.user_data["dieselgive_created_time"] = created_time

    try:
        shown_time = datetime.strptime(created_time, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M")
    except Exception:
        shown_time = created_time

    from_car = context.user_data.get("dieselgive_from_car")
    from_display = diesel_sender_display_name(from_car, context=context)
    from_label = "🚛 Дизел берган" if str(from_car or "").strip() == "Заправщик" else "🚛 Дизел берган техника номери"

    return (
        "✅ ДИЗЕЛ БЕРИШ МАЪЛУМОТЛАРИ\n\n"
        f"{from_label}: {from_display}\n"
        f"🚛 Дизел олган техника номери: {context.user_data.get('dieselgive_to_car')}\n"
        f"🕒 Вақт: {shown_time}\n"
        f"⛽ Литр: {context.user_data.get('dieselgive_liter')}\n"
        f"📝 Изоҳ: {context.user_data.get('dieselgive_note')}\n"
        "📌 Статус: Юборишга тайёр\n"
        f"📍 Спидометр/моточас: {context.user_data.get('dieselgive_speedometer_photo_id')}\n"
        "🎥 Видео: сақланди ✅\n\n"
        "Маълумот тўғрими?"
    )
    

def gas_confirm_text(context):
    created_time = context.user_data.get("gasgive_created_time") or now_text()
    context.user_data["gasgive_created_time"] = created_time

    try:
        shown_time = datetime.strptime(created_time, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M")
    except Exception:
        shown_time = created_time

    note = context.user_data.get("gasgive_note") or ""

    return (
        "✅ ГАЗ БЕРИШ МАЪЛУМОТЛАРИ\n\n"
        f"🚛 Газ берган техника номери: {context.user_data.get('gasgive_from_car')}\n"
        f"🚛 Газ олган техника номери: {context.user_data.get('gasgive_to_car')}\n"
        f"🕒 Вақт: {shown_time}\n"
        f"📝 Изоҳ: {note}\n"
        "🎥 Видео: сақланди ✅\n\n"
        "Маълумот тўғрими?"
    )


def cancel_gas_auto_confirm_task(context):
    job_name = context.user_data.get("gas_auto_confirm_job_name")

    if job_name and context.job_queue:
        for job in context.job_queue.get_jobs_by_name(job_name):
            job.schedule_removal()

    context.user_data.pop("gas_auto_confirm_job_name", None)
    context.user_data.pop("gas_auto_confirm_token", None)


def schedule_gas_auto_confirm_task(context, user_id):
    cancel_gas_auto_confirm_task(context)

    if not context.job_queue:
        print("[gas_auto_confirm] JobQueue topilmadi. python-telegram-bot[job-queue] o'rnatilganini tekshiring.")
        return

    token = str(datetime.now(TASHKENT_TZ).timestamp())
    job_name = f"gas_auto_confirm_{user_id}_{token}"

    context.user_data["gas_auto_confirm_token"] = token
    context.user_data["gas_auto_confirm_job_name"] = job_name

    context.job_queue.run_once(
        auto_confirm_gas_transfer,
        when=900,  # 15 минут
        data={
            "user_id": user_id,
            "token": token,
        },
        name=job_name,
        chat_id=user_id,
        user_id=user_id,
    )


def schedule_gas_auto_accept_task(context, transfer_id):
    if not context.job_queue:
        print("[gas_auto_accept] JobQueue topilmadi. python-telegram-bot[job-queue] o'rnatilganini tekshiring.")
        return

    job_name = f"gas_auto_accept_{transfer_id}"

    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()

    context.job_queue.run_once(
        auto_accept_gas_transfer,
        when=21600,  # 6 соат
        data={
            "transfer_id": transfer_id,
        },
        name=job_name,
    )


def get_driver_by_car(car):
    car_key = (car or "").strip().lower()
    cache_key = f"driver_by_car:{car_key}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT telegram_id, name, surname
        FROM drivers
        WHERE LOWER(car) = LOWER(%s)
          AND status = 'Тасдиқланди'
        LIMIT 1
    """, (car,))

    return cache_set(cache_key, cursor.fetchone())

def short_driver_name(row):
    if not row:
        return ""

    name = row[1] or ""
    surname = row[2] or ""

    if name:
        return f"{surname} {name[0]}."

    return surname

async def send_gas_transfer_to_receiver(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at
        FROM gas_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at = row

    if not to_driver_id:
        return

    from_driver_name = ""
    if str(from_car or "").strip() != "Заправщик":
        from_driver_name = short_driver_name(get_driver_by_car(from_car))

    to_driver_name = short_driver_name(get_driver_by_car(to_car))

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
    note = note or ""

    message_text = (
        "⛽ Сизга ГАЗ берилди\n\n"
        f"🕒 Вақт: {created_text}\n"
        f"🏢 Фирма: {firm}\n"
        f"🚛 Газ берган: {from_car} — {from_driver_name}\n"
        f"🚛 Газ олган: {to_car} — {to_driver_name}\n"
        f"📝 Изоҳ: {note}\n\n"
        "Маълумотни тасдиқлайсизми?"
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text,
        reply_markup=gas_receiver_confirm_keyboard(transfer_id)
    )

    schedule_gas_auto_accept_task(context, transfer_id)


async def notify_gas_sender_confirmed(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_car
        FROM gas_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    from_driver_id, from_car, to_car = row

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "✅ Газ бериш маълумотингиз тасдиқланди.\n\n"
            f"🚛 Газ берган техника: {from_car}\n"
            f"🚛 Газ олган техника: {to_car}"
        )
    )


async def notify_gas_sender_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            from_driver_id,
            from_car,
            to_car,
            firm,
            note,
            created_at
        FROM gas_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    from_driver_id, from_car, to_car, firm, note, created_at = row
    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
    note = note or ""
    reason = reason or "Кўрсатилмаган"

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "❌ ГАЗ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Газ берган техника: {from_car}\n"
            f"🚛 Газ олган техника: {to_car}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reason}\n"
            "📌 Статус: Рад этилди\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=gas_rejected_sender_keyboard(transfer_id)
    )


async def auto_confirm_gas_transfer(context):
    job_data = context.job.data or {}
    user_id = job_data.get("user_id")
    token = job_data.get("token")

    if context.user_data.get("gas_auto_confirm_token") != token:
        return

    if context.user_data.get("gasgive_sent"):
        return

    if context.user_data.get("mode") not in ["gasgive_confirm", "gasgive_edit_menu", "gasgive_edit_note_text", "gasgive_video", "gasgive_edit_firm", "gasgive_edit_car"]:
        return

    context.user_data["gasgive_sent"] = True

    from_car = context.user_data.get("gasgive_from_car")
    to_car = context.user_data.get("gasgive_to_car")
    firm = context.user_data.get("gasgive_firm")
    note = context.user_data.get("gasgive_note")
    video_id = context.user_data.get("gasgive_video_id")

    receiver = get_driver_by_car(to_car)

    if receiver:
        to_driver_id = receiver[0]
        transfer_status = "Қабул қилувчи текширувида"
        answered_sql = "NULL"
    else:
        to_driver_id = None
        transfer_status = "Тасдиқланди"
        answered_sql = "NOW()"

    cursor.execute(f"""
        INSERT INTO gas_transfers (
            from_driver_id,
            from_car,
            to_driver_id,
            to_car,
            firm,
            note,
            video_id,
            status,
            answered_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, {answered_sql})
        RETURNING id
    """, (
        user_id,
        from_car,
        to_driver_id,
        to_car,
        firm,
        note,
        video_id,
        transfer_status
    ))

    transfer_id = cursor.fetchone()[0]
    conn.commit()

    confirm_message_id = context.user_data.get("gasgive_confirm_message_id")

    if confirm_message_id:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=user_id,
                message_id=confirm_message_id,
                reply_markup=None
            )
        except Exception:
            pass

    if receiver:
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ Маълумот автоматик тасдиқланди ва газ олувчи ҳайдовчига юборилди."
        )
        await send_gas_transfer_to_receiver(context, transfer_id)
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "✅ Маълумот автоматик тасдиқланди.\n"
                "🚛 Газ олувчи техникада ҳайдовчи йўқлиги учун газ бериш маълумоти автоматик қабул қилинди."
            )
        )

    context.user_data.clear()
    context.user_data["mode"] = "fuel_menu"


async def auto_accept_gas_transfer(context):
    job_data = context.job.data or {}
    transfer_id = job_data.get("transfer_id")

    if not transfer_id:
        return

    cursor.execute("""
        SELECT status, from_driver_id, to_car
        FROM gas_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    status, from_driver_id, to_car = row

    if status != "Қабул қилувчи текширувида":
        return

    cursor.execute("""
        UPDATE gas_transfers
        SET status = %s, answered_at = NOW()
        WHERE id = %s
    """, ("Автоматик тасдиқланди", transfer_id))

    conn.commit()

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=f"✅ Газ бериш маълумотингиз автоматик тасдиқланди.\n🚛 Техника: {to_car}"
    )

def push_state(context, new_mode):
    if "history" not in context.user_data:
        context.user_data["history"] = []

    current_mode = context.user_data.get("mode")

    if current_mode and current_mode != new_mode:
        context.user_data["history"].append(current_mode)

    context.user_data["mode"] = new_mode


def history_period_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Охирги 10 кун", callback_data="period|10")],
        [InlineKeyboardButton("📆 Охирги 30 кун", callback_data="period|30")],
        [InlineKeyboardButton("🗓 Шу ой", callback_data="period|this_month")],
        [InlineKeyboardButton("📌 Ўтган ой", callback_data="period|last_month")],
        [InlineKeyboardButton("📆 Санадан–санагача", callback_data="period|custom")],
    ])


def confirm_action_keyboard(car):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Соз, тасдиқлаш", callback_data=f"approve|{car}")],
        [InlineKeyboardButton("❌ Носозга қайтариш", callback_data=f"reject|{car}")],
    ])


def final_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="final_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="final_edit"),
        ]
    ])

def fuel_gas_final_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👁 Кўриш", callback_data="fuel_gas_view"),
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="fuel_gas_confirm"),
        ],
        [
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="fuel_gas_edit"),
            InlineKeyboardButton("❌ Отмен", callback_data="fuel_gas_cancel"),
        ]
    ])

def fuel_gas_after_action_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="fuel_gas_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="fuel_gas_edit"),
        ],
        [
            InlineKeyboardButton("❌ Отмен", callback_data="fuel_gas_cancel"),
        ]
    ])


def fuel_gas_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Спидометр", callback_data="fuel_gas_edit_km")],
        [InlineKeyboardButton("🎥 Видео", callback_data="fuel_gas_edit_video")],
        [InlineKeyboardButton("📷 Расм", callback_data="fuel_gas_edit_photo")],
    ])


def fuel_gas_confirm_text(context):
    
    return (
        "✅ ГАЗ ОЛИШ МАЪЛУМОТЛАРИ\n\n"
        f"🚛 Техника: {context.user_data.get('fuel_car')}\n"
        f"⛽ Ёқилғи тури: {context.user_data.get('fuel_type')}\n"
        f"📍 Спидометр: {context.user_data.get('fuel_km')} км\n"
        f"🎥 Видео: сақланди ✅\n"
        f"📷 Расм: сақланди ✅\n\n"
        "Маълумот тўғрими?"
    )


def edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ КМ/Моточас", callback_data="edit|km")],
        [InlineKeyboardButton("📷 Расм", callback_data="edit|photo")],
        [InlineKeyboardButton("📝 Изоҳ", callback_data="edit|note")],
        [InlineKeyboardButton("🎥 Видео", callback_data="edit|video")],
    ])


def edit_keyboard_remove():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Изоҳ", callback_data="edit|note")],
        [InlineKeyboardButton("🎥 Видео", callback_data="edit|video")],
    ])


def get_all_cars():
    cached = cache_get("cars:all")
    if cached is not None:
        return cached

    cursor.execute("""
        SELECT firm, car_number, car_type, status, fuel_type
        FROM cars
        ORDER BY firm, car_number
    """)

    rows = cursor.fetchall()
    result = []

    for row in rows:
        result.append([
            row[0] or "",   # 0 firm
            row[1] or "",   # 1 car_number
            row[2] or "",   # 2 car_type
            "",             # 3
            "",             # 4
            "",             # 5
            row[3] or "",   # 6 status
            row[4] or ""    # 7 fuel_type
        ])

    return cache_set("cars:all", result)

def get_car_type(car):
    for row in get_all_cars():
        if len(row) > 2 and row[1].strip().lower() == car.strip().lower():
            return row[2].strip()
    return ""

def get_car_fuel_type(car):
    for row in get_all_cars():
        if len(row) > 7 and row[1].strip().lower() == car.strip().lower():
            return row[7].strip()
    return ""



def get_driver_diesel_received_totals(user_id):
    """Ҳайдовчи қабул қилган дизел: жорий ой ва умумий жами.
    Фақат status='Тасдиқланди' бўлган дизеллар ҳисобланади.
    """
    cache_key = f"driver:diesel_received_totals:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN created_at >= date_trunc('month', NOW())
                        THEN CASE WHEN COALESCE(liter, '') ~ '^[0-9]+$' THEN liter::INTEGER ELSE 0 END
                        ELSE 0
                    END
                ), 0) AS month_liter,
                COALESCE(SUM(
                    CASE WHEN COALESCE(liter, '') ~ '^[0-9]+$' THEN liter::INTEGER ELSE 0 END
                ), 0) AS total_liter
            FROM diesel_transfers
            WHERE to_driver_id = %s
              AND status = 'Тасдиқланди'
        """, (int(user_id),))

        row = cursor.fetchone()
        result = (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)
        return cache_set(cache_key, result)

    except Exception as e:
        print("GET DRIVER DIESEL RECEIVED TOTALS ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return (0, 0)


def get_driver_diesel_given_monthly(user_id):
    """Ҳайдовчи ой давомида берган дизел.
    Фақат status='Тасдиқланди' бўлган дизеллар ҳисобланади.
    """
    cache_key = f"driver:diesel_given_monthly:{int(user_id)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        cursor.execute("""
            SELECT COALESCE(SUM(
                CASE WHEN COALESCE(liter, '') ~ '^[0-9]+$' THEN liter::INTEGER ELSE 0 END
            ), 0)
            FROM diesel_transfers
            WHERE from_driver_id = %s
              AND status = 'Тасдиқланди'
              AND created_at >= date_trunc('month', NOW())
        """, (int(user_id),))

        row = cursor.fetchone()
        result = int(row[0] or 0) if row else 0
        return cache_set(cache_key, result)

    except Exception as e:
        print("GET DRIVER DIESEL GIVEN MONTHLY ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0



def get_driver_diesel_report_counts(user_id):
    """Ҳайдовчи ҳисобот менюси учун тасдиқланган дизел приход/расход сонлари."""
    try:
        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN to_driver_id = %s THEN 1 ELSE 0 END), 0) AS received_count,
                COALESCE(SUM(CASE WHEN from_driver_id = %s THEN 1 ELSE 0 END), 0) AS given_count
            FROM diesel_transfers
            WHERE status = 'Тасдиқланди'
              AND (to_driver_id = %s OR from_driver_id = %s)
        """, (int(user_id), int(user_id), int(user_id), int(user_id)))
        row = cursor.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
    except Exception as e:
        print("DRIVER DIESEL REPORT COUNTS ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0, 0


def get_driver_gas_report_counts(user_id):
    """Ҳайдовчи ҳисобот менюси учун тасдиқланган газ приход/расход сонлари."""
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM fuel_reports
            WHERE telegram_id = %s
              AND LOWER(COALESCE(fuel_type, '')) = LOWER('Газ')
        """, (int(user_id),))
        received_row = cursor.fetchone()
        received_count = int(received_row[0] or 0) if received_row else 0

        cursor.execute("""
            SELECT COUNT(*)
            FROM gas_transfers
            WHERE from_driver_id = %s
              AND status = 'Тасдиқланди'
        """, (int(user_id),))
        given_row = cursor.fetchone()
        given_count = int(given_row[0] or 0) if given_row else 0

        return received_count, given_count
    except Exception as e:
        print("DRIVER GAS REPORT COUNTS ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0, 0


def driver_reports_keyboard(user_id):
    driver_car = get_driver_car(user_id)
    fuel_type = str(get_car_fuel_type(driver_car) or "").strip().lower()

    if fuel_type == "газ":
        gas_received_count, gas_given_count = get_driver_gas_report_counts(user_id)
        return ReplyKeyboardMarkup([
            [KeyboardButton(f"📥 Приход ГАЗ маълумотлари (Рўйхатда {gas_received_count} та)")],
            [KeyboardButton(f"📤 Расход ГАЗ маълумотлари (Рўйхатда {gas_given_count} та)")],
            [KeyboardButton("⬅️ Орқага")],
        ], resize_keyboard=True)

    received_count, given_count = get_driver_diesel_report_counts(user_id)
    return ReplyKeyboardMarkup([
        [KeyboardButton(f"📥 Приход Дизел маълумотлари (Рўйхатда {received_count} та)")],
        [KeyboardButton(f"📤 Расход Дизел маълумотлари (Рўйхатда {given_count} та)")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def driver_diesel_report_list_keyboard(user_id, report_type):
    """report_type: received | given"""
    try:
        if report_type == "received":
            condition = "dt.to_driver_id = %s"
            callback_prefix = "driver_diesel_report_card|received|"
        else:
            condition = "dt.from_driver_id = %s"
            callback_prefix = "driver_diesel_report_card|given|"

        cursor.execute(f"""
            SELECT
                dt.id,
                dt.created_at,
                dt.from_car,
                dt.to_car,
                COALESCE(dt.liter, '')
            FROM diesel_transfers dt
            WHERE dt.status = 'Тасдиқланди'
              AND {condition}
            ORDER BY dt.created_at DESC
        """, (int(user_id),))
        rows = cursor.fetchall()
    except Exception as e:
        print("DRIVER DIESEL REPORT LIST ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        rows = []

    keyboard = []
    for transfer_id, created_at, from_car, to_car, liter in rows:
        date_text = created_at.strftime("%d.%m") if created_at else "--.--"
        car_text = from_car if report_type == "received" else to_car
        liter_text = format_liter(liter) if 'format_liter' in globals() else str(liter or 0)
        keyboard.append([
            InlineKeyboardButton(
                f"{date_text} / {car_text or '-'} / {liter_text} л",
                callback_data=f"{callback_prefix}{transfer_id}"
            )
        ])

    if not keyboard:
        keyboard.append([InlineKeyboardButton("❌ Маълумот топилмади", callback_data="none")])

    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="driver_diesel_report_back|menu")])
    return InlineKeyboardMarkup(keyboard)


def get_driver_diesel_report_row(transfer_id):
    try:
        cursor.execute("""
            SELECT
                dt.from_driver_id,
                dt.from_car,
                dt.to_driver_id,
                dt.to_car,
                dt.firm,
                COALESCE(dt.liter, ''),
                COALESCE(dt.note, ''),
                COALESCE(dt.speedometer_photo_id, ''),
                dt.video_id,
                dt.created_at,
                dt.status,
                dt.approved_by_id
            FROM diesel_transfers dt
            WHERE dt.id = %s
        """, (int(transfer_id),))
        return cursor.fetchone()
    except Exception as e:
        print("DRIVER DIESEL REPORT ROW ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def driver_diesel_report_card_text(row, report_type):
    (
        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note,
        speedometer_value, video_id, created_at, status, approved_by_id
    ) = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
    approved_name = get_driver_full_name_by_telegram_id(approved_by_id) if approved_by_id else "Киритилмаган"
    title = "📥 ПРИХОД ДИЗЕЛ МАЪЛУМОТИ" if report_type == "received" else "📤 РАСХОД ДИЗЕЛ МАЪЛУМОТИ"

    return (
        f"{title}\n\n"
        f"🕒 Сана: {created_text}\n"
        f"🏢 Фирма номи: {firm or 'Киритилмаган'}\n"
        f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
        f"🚛 Дизел олган техника: {to_car or 'Киритилмаган'}\n"
        f"⛽ Литр: {liter or 0} л\n"
        f"📍 Спидометр/моточас: {speedometer_value or 'Киритилмаган'}\n"
        f"📝 Изоҳ: {note or ''}\n"
        f"✅ Ким тасдиқлаган: {approved_name}\n"
        f"📌 Статус: {diesel_status_display(status)}\n\n"
        "Маълумотни кўриш учун 👁 Кўриш тугмасини босинг."
    )


def driver_diesel_report_view_keyboard(report_type, transfer_id, has_media=True):
    buttons = []
    if has_media:
        buttons.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"driver_diesel_report_view|{report_type}|{transfer_id}")])
    buttons.append([InlineKeyboardButton("⬅️ Орқага", callback_data=f"driver_diesel_report_back|list|{report_type}")])
    return InlineKeyboardMarkup(buttons)



def driver_gas_report_list_keyboard(user_id, report_type):
    """report_type: received | given"""
    try:
        if report_type == "received":
            cursor.execute("""
                SELECT id, created_at, car, COALESCE(km, '')
                FROM fuel_reports
                WHERE telegram_id = %s
                  AND LOWER(COALESCE(fuel_type, '')) = LOWER('Газ')
                ORDER BY created_at DESC
            """, (int(user_id),))
            rows = cursor.fetchall()
            keyboard = []
            for record_id, created_at, car, km in rows:
                date_text = created_at.strftime("%d.%m") if created_at else "--.--"
                keyboard.append([
                    InlineKeyboardButton(
                        f"{date_text} / {car or '-'} / {km or '-'}",
                        callback_data=f"driver_gas_report_card|received|{record_id}"
                    )
                ])
        else:
            cursor.execute("""
                SELECT id, created_at, from_car, to_car
                FROM gas_transfers
                WHERE from_driver_id = %s
                  AND status = 'Тасдиқланди'
                ORDER BY created_at DESC
            """, (int(user_id),))
            rows = cursor.fetchall()
            keyboard = []
            for transfer_id, created_at, from_car, to_car in rows:
                date_text = created_at.strftime("%d.%m") if created_at else "--.--"
                keyboard.append([
                    InlineKeyboardButton(
                        f"{date_text} / {to_car or '-'}",
                        callback_data=f"driver_gas_report_card|given|{transfer_id}"
                    )
                ])
    except Exception as e:
        print("DRIVER GAS REPORT LIST ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        keyboard = []

    if not keyboard:
        keyboard.append([InlineKeyboardButton("❌ Маълумот топилмади", callback_data="none")])

    keyboard.append([InlineKeyboardButton("⬅️ Орқага", callback_data="driver_gas_report_back|menu")])
    return InlineKeyboardMarkup(keyboard)


def get_driver_gas_received_report_row(record_id):
    try:
        cursor.execute("""
            SELECT id, telegram_id, car, fuel_type, km, video_id, photo_id, created_at
            FROM fuel_reports
            WHERE id = %s
        """, (int(record_id),))
        return cursor.fetchone()
    except Exception as e:
        print("DRIVER GAS RECEIVED REPORT ROW ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def get_driver_gas_given_report_row(transfer_id):
    try:
        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_driver_id,
                to_car,
                firm,
                COALESCE(note, ''),
                video_id,
                created_at,
                status,
                approved_by_id
            FROM gas_transfers
            WHERE id = %s
        """, (int(transfer_id),))
        return cursor.fetchone()
    except Exception as e:
        print("DRIVER GAS GIVEN REPORT ROW ERROR:", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def gas_status_display(status):
    if status == "Тасдиқланди":
        return "Тасдиқланган"
    if status == "Рад этилди":
        return "Рад этилган"
    return status or "Киритилмаган"


def driver_gas_received_card_text(row):
    record_id, telegram_id, car, fuel_type, km, video_id, photo_id, created_at = row
    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
    return (
        "📥 ПРИХОД ГАЗ МАЪЛУМОТИ\n\n"
        f"🕒 Сана: {created_text}\n"
        f"🚛 Техника: {car or 'Киритилмаган'}\n"
        f"⛽ Ёқилғи тури: {fuel_type or 'Газ'}\n"
        f"📍 Спидометр: {km or 'Киритилмаган'}\n"
        f"🎥 Видео: {'сақланди ✅' if video_id else 'йўқ'}\n"
        f"📷 Расм: {'сақланди ✅' if photo_id else 'йўқ'}\n"
        "📌 Статус: Тасдиқланган\n\n"
        "Маълумотни кўриш учун 👁 Кўриш тугмасини босинг."
    )


def driver_gas_given_card_text(row):
    from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at, status, approved_by_id = row
    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
    from_name = get_driver_full_name_by_telegram_id(from_driver_id) if from_driver_id else "Киритилмаган"
    to_name = get_driver_full_name_by_telegram_id(to_driver_id) if to_driver_id else "Киритилмаган"
    approved_name = get_driver_full_name_by_telegram_id(approved_by_id) if approved_by_id else "Киритилмаган"
    return (
        "📤 РАСХОД ГАЗ МАЪЛУМОТИ\n\n"
        f"🕒 Сана: {created_text}\n"
        f"🏢 Фирма номи: {firm or 'Киритилмаган'}\n"
        f"🚛 Газ берган техника: {from_car or 'Киритилмаган'} — {from_name}\n"
        f"🚛 Газ олган техника: {to_car or 'Киритилмаган'} — {to_name}\n"
        f"📝 Изоҳ: {note or ''}\n"
        f"✅ Ким тасдиқлаган: {approved_name}\n"
        f"🎥 Видео: {'сақланди ✅' if video_id else 'йўқ'}\n"
        f"📌 Статус: {gas_status_display(status)}\n\n"
        "Маълумотни кўриш учун 👁 Кўриш тугмасини босинг."
    )


def driver_gas_report_view_keyboard(report_type, record_id, has_media=True):
    buttons = []
    if has_media:
        buttons.append([InlineKeyboardButton("👁 Кўриш", callback_data=f"driver_gas_report_view|{report_type}|{record_id}")])
    return InlineKeyboardMarkup(buttons)

def driver_menu_text(user_id):
    driver_car = get_driver_car(user_id)
    fuel_type = get_car_fuel_type(driver_car)

    text = (
        f"🚚 Ҳайдовчи менюси\n\n"
        f"🚛 Техника: {driver_car}\n"
        f"⛽ Ёқилғи тури: {fuel_type}"
    )

    # Газлик техника ҳайдовчиларида дизел ойлик/берилган қатори кўринмайди.
    if str(fuel_type or "").strip().lower() == "дизел":
        month_received_liter, _ = get_driver_diesel_received_totals(user_id)
        month_given_liter = get_driver_diesel_given_monthly(user_id)
        text += (
            f"\n⛽ Ойлик олинган дизел: {month_received_liter} л"
            f"\n⛽ Ойлик берилган дизел: {month_given_liter} л"
        )

    return text

def get_repair_stats(car):
    cursor.execute("""
        SELECT status
        FROM repairs
        WHERE LOWER(car_number) = LOWER(%s)
    """, (car,))

    rows = cursor.fetchall()

    kirgan = 0
    chiqqan = 0

    for row in rows:
        status = row[0] if row[0] else ""

        if status == "Носоз":
            kirgan += 1

        elif status in ["Текширувда", "Соз"]:
            chiqqan += 1

    return kirgan, chiqqan

def history_car_buttons_by_firm(firm):
    cursor.execute("""
        SELECT car_number, car_type
        FROM cars
        WHERE LOWER(firm) = LOWER(%s)
        ORDER BY car_number
    """, (firm,))

    cars = cursor.fetchall()

    cursor.execute("""
        SELECT
            car_number,
            COUNT(CASE WHEN status = 'Носоз' THEN 1 END) AS total_repairs,
            COUNT(CASE WHEN status = 'Соз' THEN 1 END) AS approved_repairs
        FROM repairs
        GROUP BY car_number
    """)

    stats_rows = cursor.fetchall()

    stats = {}

    for row in stats_rows:
        car_number = row[0]

        stats[car_number] = {
            "total": row[1] or 0,
            "approved": row[2] or 0
        }

    keyboard = []

    for car_number, car_type in cars:
        car_stat = stats.get(car_number, {
            "total": 0,
            "approved": 0
        })

        keyboard.append([
            InlineKeyboardButton(
                f"{car_number} | {car_type} | Р:{car_stat['total']} | Т:{car_stat['approved']}",
                callback_data=f"car|{car_number}"
            )
        ])

    if not keyboard:
        keyboard = [[
            InlineKeyboardButton(
                "❌ Техника топилмади",
                callback_data="none"
            )
        ]]

    return InlineKeyboardMarkup(keyboard)


def car_buttons_by_firm(firm, only_available_for_driver=False, exclude_driver_id=None):
    rows_for_buttons = []
    driver_car_counts = get_driver_car_counts_by_firm(firm, exclude_driver_id=exclude_driver_id) if only_available_for_driver else {}

    for row in get_all_cars():
        if len(row) < 7:
            continue

        firm_name = row[0].strip()
        car = row[1].strip()
        turi = row[2].strip()
        holat = row[6].strip()

        if firm_name.lower() == firm.strip().lower():
            if only_available_for_driver and not is_car_available_for_driver(car, driver_car_counts):
                continue
            order = STATUS_ORDER.get(holat.lower(), 99)
            rows_for_buttons.append((order, car, turi, holat))

    rows_for_buttons.sort(key=lambda x: x[0])

    keyboard = []
    for _, car, turi, holat in rows_for_buttons:
        keyboard.append([
            InlineKeyboardButton(
                f"{car} | {turi} | {holat}",
                callback_data=f"car_{car}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)

def driver_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Исм", callback_data="driver_edit|name")],
        [InlineKeyboardButton("👤 Фамилия", callback_data="driver_edit|surname")],
        [InlineKeyboardButton("📞 Телефон", callback_data="driver_edit|phone")],
        [InlineKeyboardButton("🏢 Фирма", callback_data="driver_edit|firm")],
        [InlineKeyboardButton("🚛 Техника", callback_data="driver_edit|car")],
    ])


def register_edit_keyboard(context):
    work_role = context.user_data.get("driver_work_role", "driver")

    buttons = [
        [InlineKeyboardButton("👤 Исм", callback_data="driver_edit|name")],
        [InlineKeyboardButton("👤 Фамилия", callback_data="driver_edit|surname")],
        [InlineKeyboardButton("📞 Телефон", callback_data="driver_edit|phone")],
        [InlineKeyboardButton("🪪 Лавозим", callback_data="driver_edit|role")],
    ]

    if work_role in ["driver", "mechanic"]:
        buttons.append([InlineKeyboardButton("🏢 Фирма", callback_data="driver_edit|firm")])

    if work_role == "driver":
        buttons.append([InlineKeyboardButton("🚛 Техника", callback_data="driver_edit|car")])

    return InlineKeyboardMarkup(buttons)

def car_buttons_by_firm_and_status(firm, status_filter):
    keyboard = []

    for row in get_all_cars():
        if len(row) < 7:
            continue

        firm_name = row[0].strip()
        car = row[1].strip()
        turi = row[2].strip()
        holat = row[6].strip()

        if firm_name.lower() == firm.strip().lower() and holat.lower() == status_filter.lower():
            keyboard.append([
                InlineKeyboardButton(
                    f"{car} | {turi} | {holat}",
                    callback_data=f"car|{car}"
                )
            ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Мос техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)


def cars_for_check_by_firm_group():
    rows = get_all_cars()
    keyboard = []

    for firm in FIRM_NAMES:
        firm_has_cars = False

        for row in rows:
            if len(row) < 7:
                continue

            firm_name = row[0].strip()
            car = row[1].strip()
            turi = row[2].strip()
            holat = row[6].strip()

            if firm_name.lower() == firm.lower() and holat.lower() == "текширувда":
                if not firm_has_cars:
                    keyboard.append([InlineKeyboardButton(f"🏢 {firm}", callback_data="none")])
                    firm_has_cars = True

                keyboard.append([
                    InlineKeyboardButton(
                        f"{car} | {turi} | {holat}",
                        callback_data=f"check_detail|{car}"
                    )
                ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Текширувда техника йўқ", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)

def db_execute(query, params=None):
    global conn, cursor

    try:
        cursor.execute(query, params or ())
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

        reset_db_pool()
        cursor.execute(query, params or ())
        conn.commit()


def save_repair_to_db(
    car,
    km,
    repair_type,
    status,
    note,
    video_id,
    photo_id,
    person,
    start_time,
    end_time,
    duration,
    executor_id
):
    db_execute("""
        CREATE TABLE IF NOT EXISTS repairs (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW(),
            car TEXT,
            km TEXT,
            repair_type TEXT,
            status TEXT,
            note TEXT,
            video_id TEXT,
            photo_id TEXT,
            person TEXT,
            start_time TEXT,
            end_time TEXT,
            duration TEXT,
            executor_id BIGINT
        )
    """)

    db_execute("""
        INSERT INTO repairs (
            car, km, repair_type, status, note,
            video_id, photo_id, person,
            start_time, end_time, duration, executor_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        car, km, repair_type, status, note,
        video_id, photo_id, person,
        start_time, end_time, duration, executor_id
    ))

def update_car_status(car, status):
    global conn, cursor

    try:
        cursor.execute("""
            UPDATE cars
            SET status = %s
            WHERE LOWER(car_number) = LOWER(%s)
        """, (status, car))

        conn.commit()
        clear_car_cache()
        sync_car_status_to_google_sheet(car, status)
        return True

    except Exception as e:
        print("UPDATE CAR STATUS ERROR:", e)

        try:
            conn.rollback()
        except:
            pass

        try:
            reset_db_pool()

            cursor.execute("""
                UPDATE cars
                SET status = %s
                WHERE LOWER(car_number) = LOWER(%s)
            """, (status, car))

            conn.commit()
            clear_car_cache()
            sync_car_status_to_google_sheet(car, status)
            return True

        except Exception as e2:
            print("RECONNECT UPDATE ERROR:", e2)

    return False


def clean_note(note):
    for firm in FIRM_NAMES:
        note = note.replace(f"{firm}.", "").strip()

    note = note.replace("Ремонтга қўшиш:", "").strip()
    note = note.replace("Ремонтдан чиқариш:", "").strip()

    return note


def get_last_open_repair_start_time(car):
    rows = remont_ws.get_all_values()

    for i in range(len(rows), 1, -1):
        row = rows[i - 1]

        if len(row) > 10 and row[2].strip().lower() == car.strip().lower():
            status = row[5].strip() if len(row) > 5 else ""
            start_time = row[10].strip() if len(row) > 10 else ""

            if status == "Носоз" and start_time:
                return start_time

    return ""


def get_last_repair_pair(car):
    rows = remont_ws.get_all_values()[1:]

    chiqqan = None
    kirgan_list = []

    for row in reversed(rows):
        if len(row) < 13:
            continue

        if row[2].strip().lower() != car.strip().lower():
            continue

        if row[5].strip() == "Текширувда":
            chiqqan = row
            break

    if not chiqqan:
        return [], None

    found_exit = False

    for row in reversed(rows):
        if len(row) < 13:
            continue

        if row[2].strip().lower() != car.strip().lower():
            continue

        if row == chiqqan:
            found_exit = True
            continue

        if not found_exit:
            continue

        status = row[5].strip()

        if status in ["Текширувда", "Соз"]:
            break

        if status == "Носоз":
            kirgan_list.append(row)

    kirgan_list.reverse()
    return kirgan_list, chiqqan


async def safe_send_video(bot, chat_id, file_id):
    if not file_id:
        return

    try:
        await bot.send_video_note(chat_id=chat_id, video_note=file_id)
    except Exception:
        try:
            await bot.send_video(chat_id=chat_id, video=file_id)
        except Exception:
            pass


def diesel_receiver_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_receive_view|{transfer_id}")],
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_receive_reject|{transfer_id}")
        ]
    ])


def diesel_receiver_after_view_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_receive_reject|{transfer_id}")
        ]
    ])


def diesel_rejected_sender_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_rejected_view|{transfer_id}")],
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_rejected_resend|{transfer_id}")],
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_rejected_edit|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_rejected_cancel|{transfer_id}")]
    ])


def diesel_rejected_receiver_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_receiver_rejected_view|{transfer_id}")],
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receiver_rejected_accept|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_receiver_rejected_cancel|{transfer_id}")]
    ])


async def send_diesel_transfer_to_receiver(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, created_at = row

    if to_driver_id is None:
        print(f"DIESEL TRANSFER AUTO APPROVED: transfer_id={transfer_id}, to_car={to_car}, no receiver driver")
        return

    from_driver_name = ""
    if str(from_car or "").strip() != "Заправщик":
        from_driver_name = short_driver_name(get_driver_by_car(from_car))

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    if to_driver_id is None:
        print(f"DIESEL TRANSFER RECEIVER NOT FOUND: transfer_id={transfer_id}, to_car={to_car}")
        return

    sender_text = diesel_sender_display_name(from_car, from_driver_id=from_driver_id)
    if str(from_car or "").strip() != "Заправщик" and from_driver_name:
        sender_text = f"{from_car} — {from_driver_name}"

    message_text = (
        "⛽ Сизга Дизел берилди\n\n"
        f"Кимдан: {sender_text}\n"
        f"Сана: {created_text}\n"
        f"Литр: {liter} л\n"
        "Статус: Қабул қилувчида\n\n"
        "Ёқилғи ҳисоботи → ДИЗЕЛ приход бўлимида тасдиқланг."
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text
    )


async def notify_diesel_sender_confirmed(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_car, liter
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()
    if not row:
        return

    from_driver_id, from_car, to_car, liter = row
    from_display = diesel_sender_display_name(from_car, from_driver_id=from_driver_id)
    from_label = "🚛 Дизел берган" if str(from_car or "").strip() == "Заправщик" else "🚛 Берган техника"

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "✅ ДИЗЕЛ расход маълумотингиз тасдиқланди.\n\n"
            f"{from_label}: {from_display}\n"
            f"🚛 Олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            "📌 Статус: Қабул қилинди"
        )
    )


async def notify_diesel_sender_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            from_driver_id,
            from_car,
            to_car,
            firm,
            liter,
            note,
            created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    from_driver_id, from_car, to_car, firm, liter, note, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    msg = await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "❌ ДИЗЕЛ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reason}\n"
            "📌 Статус: Рад этилди\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_sender_keyboard(transfer_id)
    )
    remember_inline_message_for_chat(context, from_driver_id, msg)

async def notify_diesel_receiver_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            to_driver_id,
            from_driver_id,
            from_car,
            to_car,
            firm,
            liter,
            note,
            video_id,
            created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    to_driver_id, from_driver_id, from_car, to_car, firm, liter, note, video_id, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    msg = await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=(
            "❌ ДИЗЕЛ ОЛИШ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган: {to_car}\n"
            f"📝 Изоҳ: {note}\n"
            f"⛽ Литр: {liter}\n\n"
            f"❗ Рад этилиш сабаби: {reason}\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_receiver_keyboard(transfer_id)
    )
    remember_inline_message_for_chat(context, to_driver_id, msg)


# === DIESEL RECEIVER CONFIRM FLOW HELPERS START ===

def diesel_receiver_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_receive_view|{transfer_id}")],
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_receive_reject|{transfer_id}")
        ]
    ])


def diesel_receiver_after_view_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"diesel_receive_reject|{transfer_id}")
        ]
    ])


def diesel_rejected_sender_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_rejected_view|{transfer_id}")],
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_rejected_resend|{transfer_id}")],
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_rejected_edit|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_rejected_cancel|{transfer_id}")]
    ])

def diesel_rejected_after_view_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_rejected_resend|{transfer_id}")],
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_rejected_edit|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен", callback_data=f"diesel_rejected_cancel|{transfer_id}")]
    ])

def diesel_rejected_receiver_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"diesel_receiver_rejected_view|{transfer_id}")],
        [InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receiver_rejected_accept|{transfer_id}")],
        [InlineKeyboardButton("❌ Отмен қилиш", callback_data=f"diesel_receiver_rejected_cancel|{transfer_id}")]
    ])


async def send_diesel_transfer_to_receiver(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, created_at = row

    from_driver_name = ""
    if str(from_car or "").strip() != "Заправщик":
        from_driver_name = short_driver_name(get_driver_by_car(from_car))

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    if to_driver_id is None:
        print(f"DIESEL TRANSFER RECEIVER NOT FOUND: transfer_id={transfer_id}, to_car={to_car}")
        return

    sender_text = diesel_sender_display_name(from_car, from_driver_id=from_driver_id)
    if str(from_car or "").strip() != "Заправщик" and from_driver_name:
        sender_text = f"{from_car} — {from_driver_name}"

    message_text = (
        "⛽ Сизга Дизел берилди\n\n"
        f"Кимдан: {sender_text}\n"
        f"Сана: {created_text}\n"
        f"Литр: {liter} л\n"
        "Статус: Қабул қилувчида\n\n"
        "Ёқилғи ҳисоботи → ДИЗЕЛ приход бўлимида тасдиқланг."
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text
    )


async def notify_diesel_sender_confirmed(context, transfer_id):
    cursor.execute("""
        SELECT from_driver_id, from_car, to_car, liter
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    from_driver_id, from_car, to_car, liter = row

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "✅ ДИЗЕЛ расход маълумотингиз тасдиқланди.\n\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            "📌 Статус: Қабул қилинди"
        )
    )


async def notify_diesel_sender_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            from_driver_id,
            from_car,
            to_car,
            firm,
            liter,
            note,
            created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    from_driver_id, from_car, to_car, firm, liter, note, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    await context.bot.send_message(
        chat_id=int(from_driver_id),
        text=(
            "❌ ДИЗЕЛ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reason}\n"
            "📌 Статус: Рад этилди\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_sender_keyboard(transfer_id)
    )

async def notify_diesel_receiver_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            to_driver_id,
            from_driver_id,
            from_car,
            to_car,
            firm,
            liter,
            note,
            video_id,
            created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    to_driver_id, from_driver_id, from_car, to_car, firm, liter, note, video_id, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()


# === DIESEL RECEIVER CONFIRM FLOW HELPERS END ===


async def safe_send_photo(bot, chat_id, file_id):
    if not file_id:
        return

    try:
        await bot.send_photo(chat_id=chat_id, photo=file_id)
    except Exception:
        pass


async def send_last_repairs(query, car, repair_type):
    rows = remont_ws.get_all_values()[1:]
    result = []
    one_year_ago = datetime.now(TASHKENT_TZ) - timedelta(days=365)

    for row in rows:
        if len(row) < 14:
            continue

        if row[2].strip().lower() != car.strip().lower():
            continue

        if row[4].strip().lower() != repair_type.strip().lower():
            continue

        sana_text = row[1].strip()

        try:
            sana = datetime.strptime(sana_text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        if sana < one_year_ago.replace(tzinfo=None):
            continue

        result.append({
            "date": sana_text,
            "km": row[3] if len(row) > 3 else "",
            "note": clean_note(row[6] if len(row) > 6 else ""),
            "video_id": row[7] if len(row) > 7 else "",
        })

    result = result[-3:]

    if not result:
        await query.message.reply_text("ℹ️ Охирги 1 йилда бу ремонт тури бўйича маълумот топилмади.")
        return

    await query.message.reply_text("⚠️ Охирги 1 йилда ушбу ремонт тури бўйича охирги 3 та иш:")

    for item in result:
        await query.message.reply_text(
            f"📅 Сана: {item['date']}\n"
            f"⏱ КМ/Моточас: {item['km']}\n"
            f"📝 Изоҳ: {item['note']}"
        )

        if item["video_id"]:
            media_key = f"history_{car}_{item['date']}"

            await query.message.reply_text(
                "📎 Видео мавжуд",
                reply_markup=view_media_keyboard(media_key)
            )


async def send_history_by_date(message, car, start_date, end_date):
    car_type = get_car_type(car)

    def to_dt(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        try:
            return datetime.strptime(str(value).split(".")[0], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    start_dt = start_date.replace(tzinfo=None)
    end_dt = end_date.replace(tzinfo=None)

    cursor.execute("""
        SELECT
            id, car_number, km, repair_type, status, comment,
            enter_photo, enter_video, entered_by, exited_by,
            approved_by, entered_at, exited_at, approved_at
        FROM repairs
        WHERE LOWER(car_number) = LOWER(%s)
        ORDER BY id ASC
    """, (car,))

    rows = cursor.fetchall()

    events = []
    open_repairs = []
    pending_exit = None

    for row in rows:
        row_id = row[0]
        car_number = row[1] or ""
        km = row[2] or ""
        repair_type = row[3] or ""
        status = row[4] or ""
        comment = row[5] or ""
        photo_id = row[6] or ""
        video_id = row[7] or ""
        entered_by = row[8] or ""
        exited_by = row[9] or ""
        approved_by = row[10] or ""
        entered_at = to_dt(row[11])
        exited_at = to_dt(row[12])
        approved_at = to_dt(row[13])

        if status == "Носоз" and entered_at:
            if start_dt <= entered_at <= end_dt:
                events.append({
                    "time": entered_at,
                    "type": "enter",
                    "row_id": row_id,
                    "text": (
                        f"🔴 Ремонтга кирган\n\n"
                        f"🚘 {car_number}\n"
                        f"🔧 Тури: {car_type}\n"
                        f"📅 Сана: {entered_at.strftime('%d-%m-%Y %H:%M')}\n"
                        f"📟 KM/Моточас: {km}\n"
                        f"🛠 Ремонт: {repair_type}\n"
                        f"📌 Статус: Носоз\n"
                        f"💬 Изоҳ: {comment}\n"
                        f"👨‍🔧 Киритган: {entered_by}"
                    ),
                    "photo_id": photo_id,
                    "video_id": video_id
                })

            open_repairs.append(row)
            continue

        if status == "Текширувда":
            pending_exit = row
            continue

        if status == "Соз":
            if not pending_exit:
                continue

            exit_time = to_dt(pending_exit[12]) or to_dt(pending_exit[11])
            if not exit_time:
                continue

            if not (start_dt <= exit_time <= end_dt):
                pending_exit = None
                open_repairs = []
                continue

            exit_comment = pending_exit[5] or ""
            exit_video_id = pending_exit[7] or ""
            exit_person = pending_exit[9] or pending_exit[8] or ""

            first_start = None
            if open_repairs:
                first_start = to_dt(open_repairs[0][11])

            duration_text = ""
            if first_start:
                duration_text = calculate_duration(
                    first_start.strftime("%Y-%m-%d %H:%M:%S"),
                    exit_time.strftime("%Y-%m-%d %H:%M:%S")
                )

            repair_types = ", ".join([r[3] for r in open_repairs if r[3]]) or "Ремонтдан чиқарилди"

            events.append({
                "time": exit_time,
                "type": "exit",
                "row_id": pending_exit[0],
                "text": (
                    f"🟢 Ремонтдан чиққан\n\n"
                    f"🚘 {car_number}\n"
                    f"🔧 Тури: {car_type}\n"
                    f"📅 Сана: {exit_time.strftime('%d-%m-%Y %H:%M')}\n"
                    f"⏳ Ремонт учун кетган вақт: {duration_text}\n"
                    f"🛠 Ремонт: {repair_types}\n"
                    f"📌 Статус: тасдиқланди\n"
                    f"💬 Изоҳ: {exit_comment}\n"
                    f"👨‍🔧 Чиқарган: {exit_person}\n"
                    f"👤 Текширувчи: {approved_by}"
                ),
                "photo_id": "",
                "video_id": exit_video_id
            })

            pending_exit = None
            open_repairs = []

    events.sort(key=lambda x: x["time"])

    if not events:
        await message.reply_text(
            "Бу вақт оралиғида ремонт историяси топилмади.\n\nБошқа даврни танланг:",
            reply_markup=history_period_keyboard()
        )
        return

    for item in events:
        await message.reply_text(item["text"])

        if item["photo_id"] or item["video_id"]:
            if item["type"] == "enter":
                media_key = f"history_enter_{item['row_id']}"
                media_text = "📎 Расм/видеони кўриш учун:"
            else:
                media_key = f"history_exit_{item['row_id']}"
                media_text = "📎 Видеони кўриш учун:"

            await message.reply_text(
                media_text,
                reply_markup=view_media_keyboard(media_key)
            )

async def notify_technadzor_for_check(context, car):
    kirgan_list, chiqqan = get_last_repair_pair(car)

    if not chiqqan:
        return

    for user_id in get_user_ids_by_role("technadzor"):
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🔔 Янги техника текширувга келди:\n\n🚛 Техника: {car}\n🚜 Тури: {get_car_type(car)}"
            )

            if kirgan_list:
                for kirgan in kirgan_list:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            "🔴 РЕМОНТГА КИРГАН\n"
                            f"📅 Сана ва вақт: {kirgan[10] if len(kirgan) > 10 else kirgan[1]}\n"
                            f"⏱ КМ/Моточас: {kirgan[3] if len(kirgan) > 3 else ''}\n"
                            f"🔧 Ремонт тури: {kirgan[4] if len(kirgan) > 4 else ''}\n"
                            f"📝 Изоҳ: {clean_note(kirgan[6] if len(kirgan) > 6 else '')}\n"
                            f"👤 Киритган: {kirgan[9] if len(kirgan) > 9 else ''}"
                        )
                    )

                    if (len(kirgan) > 8 and kirgan[8]) or (len(kirgan) > 7 and kirgan[7]):
                        await context.bot.send_message(
                            chat_id=user_id,
                            text="📎 Расм/видеони кўриш учун:",
                            reply_markup=view_media_keyboard(f"tech_enter_{kirgan[0]}")
                        )
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "🟡 РЕМОНТДАН ЧИҚҚАН\n"
                    f"📅 Сана ва вақт: {chiqqan[11] if len(chiqqan) > 11 else chiqqan[1]}\n"
                    f"📝 Изоҳ: {clean_note(chiqqan[6] if len(chiqqan) > 6 else '')}\n"
                    f"⏳ Кетган вақт: {chiqqan[12] if len(chiqqan) > 12 else ''}\n"
                    f"👤 Чиқарган: {chiqqan[9] if len(chiqqan) > 9 else ''}"
                )
            )

            if len(chiqqan) > 7 and chiqqan[7]:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="📎 Видеони кўриш учун:",
                    reply_markup=view_media_keyboard(f"tech_exit_{chiqqan[0]}")
                )

            await context.bot.send_message(
                chat_id=user_id,
                text="Текширув натижасини танланг:",
                reply_markup=confirm_action_keyboard(car)
            )

        except Exception:
            pass
            
async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Сизнинг ID: {update.effective_user.id}"
    )

async def clear_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Фойдаланувчидаги жорий state ва сақланган inline/history маълумотларни тозалайди."""
    chat_id = update.effective_chat.id

    # /clear командасининг ўзини ўчиришга ҳаракат қиламиз.
    if update.message:
        try:
            await update.message.delete()
        except Exception:
            pass

    # Бот сақлаб қолган preview/inline хабарларини ўчиришга ҳаракат қиламиз.
    message_ids = set()
    for key in [
        "gasgive_confirm_message_id",
        "dieselgive_confirm_message_id",
        "fuel_gas_confirm_message_id",
    ]:
        value = context.user_data.get(key)
        if value:
            message_ids.add(value)

    for message_id in list(context.user_data.get("bot_message_ids", [])):
        if message_id:
            message_ids.add(message_id)

    for message_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=int(message_id))
        except Exception:
            pass

    cancel_gas_auto_confirm_task(context)
    context.user_data.clear()
    context.user_data["history"] = []
    context.user_data["inline_disabled_by_start"] = True

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text="✅ Чат тозаланди. Янги меню учун /start босинг.",
        reply_markup=ReplyKeyboardRemove()
    )
    context.user_data["bot_message_ids"] = [msg.message_id]



def v57_is_protected_mode(mode):
    protected_keywords = [
        "edit", "confirm", "video", "photo", "note", "liter", "km",
        "reject", "prihod", "dieselgive", "other_diesel",
        "repair", "driver_"
    ]
    mode_text = str(mode or "")
    return any(k in mode_text for k in protected_keywords)


async def v57_check_inactivity(update, context):
    try:
        role = get_role(update)
        mode = context.user_data.get("mode")

        if v57_is_protected_mode(mode):
            context.user_data["last_activity"] = datetime.now(TASHKENT_TZ)
            return False

        now = datetime.now(TASHKENT_TZ)
        last = context.user_data.get("last_activity")
        context.user_data["last_activity"] = now

        if not last:
            return False

        limit = timedelta(minutes=15 if role == "technadzor" else 30)

        if now - last <= limit:
            return False

        context.user_data.clear()
        context.user_data["last_activity"] = now

        if role == "technadzor":
            await update.effective_message.reply_text("⌛ Сеанс тугади.", reply_markup=technadzor_keyboard())
        elif role == "zapravshik":
            await update.effective_message.reply_text("⌛ Сеанс тугади.", reply_markup=zapravshik_main_keyboard())
        elif role == "mechanic":
            await update.effective_message.reply_text("⌛ Сеанс тугади.", reply_markup=firm_keyboard())
        elif role == "driver":
            await update.effective_message.reply_text("⌛ Сеанс тугади.", reply_markup=main_menu())
        return True
    except Exception as e:
        print("INACTIVITY CHECK ERROR:", e)
        return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    # V22: /start босилганда текширувчи, механик, заправщик ва ҳайдовчида
    # юқорида қолган эски inline кнопкалар хавфсиз ўчирилади.
    # Муҳим: clear user_data'дан ОЛДИН, чунки inline message_id'лар user_data'да сақланади.
    if get_role(update) in ["technadzor", "mechanic", "zapravshik", "driver"]:
        await clear_all_inline_messages(context, update.effective_chat.id)

    context.user_data.clear()
    context.user_data["history"] = []

    # /start босилганда эски inline кнопкалар блокланади.
    # Кейин фойдаланувчи янги менюдан танлов қилганда handle_message бу блокни очади.
    context.user_data["inline_disabled_by_start"] = True

    role = get_role(update)
    driver_status = get_driver_status(update.effective_user.id)

    if role is None:

        if driver_status == "Текширувда":
            await update.message.reply_text("⏳ Сизнинг аризангиз текширувда.")
            return

        elif driver_status == "Рад этилди":
            await update.message.reply_text("❌ Сизнинг аризангиз рад этилган.")
            return

        elif driver_status == "Тасдиқланди":
            work_role = get_driver_work_role(update.effective_user.id)

            if work_role == "zapravshik":
                await update.message.reply_text(
                    zapravka_info_text(),
                    reply_markup=zapravshik_main_keyboard()
                )
                return

            if work_role == "mechanic":
                await update.message.reply_text(
                    "🔧 Механик менюси\n\nАввал фирмани танланг:",
                    reply_markup=firm_keyboard()
                )
                return


            driver_car = get_driver_car(update.effective_user.id)
            fuel_type = get_car_fuel_type(driver_car)

            await update.message.reply_text(
                driver_menu_text(update.effective_user.id),
                reply_markup=driver_main_keyboard(fuel_type)
            )
            return

        else:
            await update.message.reply_text(
                "👤 Ким бўлиб ишлайсиз?",
                reply_markup=register_role_keyboard()
            )
            context.user_data["mode"] = "choose_work_role"
            return
            
    # V16: Ҳайдовчи тасдиқланган бўлса, уни директор блокига туширмаслик керак.
    # Структура сақланади: фақат driver role учун ўз менюси очилади.
    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik", "driver"]:
        await deny(update)
        return

    if role == "driver":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)
        await update.message.reply_text(
            driver_menu_text(update.effective_user.id),
            reply_markup=driver_main_keyboard(fuel_type)
        )
        return

    if role == "technadzor":
        await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
        return

    if role == "mechanic":
        await update.message.reply_text("🔧 Механик менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
        return

    if role == "zapravshik":
        await update.message.reply_text(zapravka_info_text(), reply_markup=zapravshik_main_keyboard())
        return

    if role == "slesar":
        await update.message.reply_text("🛠 Слесарь менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
        return

    if role == "director":
        await update.message.reply_text("👨‍💼 Директор менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
        return


async def save_final_data(update_or_query, context, message_obj):
    car = context.user_data.get("car")
    note = context.user_data.get("note", "")
    repair_type = context.user_data.get("repair_type")
    operation = context.user_data.get("operation")
    km = context.user_data.get("km", "")
    km_photo_id = context.user_data.get("km_photo_id", "")
    video_id = context.user_data.get("video_id", "")

    added_by = get_user_name(update_or_query)
    current_time = now_text()
    executor_id = update_or_query.effective_user.id
    role = get_role(update_or_query)

    if operation == "add":
        status = "Носоз"
        amal = repair_type
        repair_start_time = current_time
        repair_end_time = ""
        repair_duration = ""
    else:
        status = "Текширувда"
        amal = "Ремонтдан чиқарилди"
        repair_start_time = get_last_open_repair_start_time(car)
        repair_end_time = current_time
        repair_duration = calculate_duration(repair_start_time, repair_end_time)

    update_car_status(car, status)

    row_id = len(remont_ws.get_all_values())

    remont_ws.append_row([
        row_id,              # A
        current_time,        # B
        car,                 # C
        km,                  # D
        amal,                # E
        status,              # F
        note,                # G
        video_id,            # H
        km_photo_id,         # I
        added_by,            # J
        repair_start_time,   # K
        repair_end_time,     # L
        repair_duration,     # M
        executor_id          # N
    ])

    save_new_repair_to_db(
        car_number=car,
        km=km,
        repair_type=amal,
        status=status,
        comment=note,
        video_id=video_id,
        photo_id=km_photo_id,
        person=added_by,
        entered_at=repair_start_time,
        exited_at=repair_end_time
    )

    
    if operation == "remove":
        await notify_technadzor_for_check(context, car)

    if operation == "add":
        result_text = (
            "✅ Маълумот сақланди.\n\n"
            f"Вақт: {current_time}\n\n"
            f"🚛 Техника: {car}\n\n"
            f"📌 Ҳолат: {status}"
        )
    else:
        result_text = (
            "✅ Маълумот сақланди ва текширувга юборилди.\n\n"
            f"Вақт: {current_time}\n\n"
            f"🚛 Техника: {car}\n\n"
            f"📌 Ҳолат: {status}"
        )
    
    await message_obj.reply_text(
        result_text,
        reply_markup=technadzor_keyboard() if role == "technadzor" else action_keyboard()
    )
    
    saved_firm = context.user_data.get("firm")
    context.user_data.clear()

    if role != "technadzor":
        context.user_data["firm"] = saved_firm




async def show_driver_confirm(message, context):
    context.user_data["inline_disabled_by_start"] = False
    work_role = context.user_data.get("driver_work_role", "driver")

    role_titles = {
        "driver": "Ҳайдовчи",
        "mechanic": "Механик",
        "zapravshik": "Заправщик",
    }

    text = (
        "📋 Маълумотларни текширинг:\n\n"
        f"👤 Лавозим: {role_titles.get(work_role, work_role)}\n"
        f"👤 Исм: {context.user_data.get('driver_name', '')}\n"
        f"👤 Фамилия: {context.user_data.get('driver_surname', '')}\n"
        f"📞 Телефон: {context.user_data.get('phone', '')}\n"
    )

    if work_role == "mechanic":
        text += f"🏢 Фирма: {context.user_data.get('driver_firm', '')}\n"

    if work_role == "driver":
        text += (
            f"🏢 Фирма: {context.user_data.get('driver_firm', '')}\n"
            f"🚛 Техника: {context.user_data.get('driver_car', '')}\n"
        )

    text += "\nТасдиқлайсизми?"

    await message.reply_text(
        "✅ Маълумот танланди.",
        reply_markup=ReplyKeyboardRemove()
    )

    await message.reply_text(
        text + "\n\nТанланг:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Тасдиқлаш", callback_data="confirm_driver"),
                InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="edit_driver")
            ]
        ])
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    if not update.message or not update.message.text:
        return

    if update.message.contact:
        return

    text = update.message.text.strip()
    context.user_data["inline_disabled_by_start"] = False
    mode = context.user_data.get("mode")
    current_role = get_role(update)

    # === V51: Текширувчи дизел тарихи карточкасидан Бош менюга хавфсиз қайтиш ===
    if text == "🏠 Бош меню" and current_role == "technadzor":
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data.clear()
        context.user_data["mode"] = "technadzor_main"
        await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
        return

    # === V22: Заправшик, ҳайдовчи, механик ва текширувчи меню алмашганда
    # юқоридаги эски inline кнопкалар хавфсиз ўчсин ===
    if current_role in ["zapravshik", "driver", "mechanic", "technadzor"] and text in [
        "⛽ Ёқилғи ҳисоботи",
        "📊 Ҳисоботлар",
        "📥 Приход Дизел маълумотлари",
        "📤 Расход Дизел маълумотлари",
        "📥 Приход ГАЗ маълумотлари",
        "📤 Расход ГАЗ маълумотлари",
        "✅ ДИЗЕЛ приход",
        "⛽ ДИЗЕЛ расход",
        "⛽ ГАЗ олиш",
        "⛽ ГАЗ бериш",
        "🟡 Дизел бўлими",
        "➕ Дизел приход",
        "➖ Дизел расход",
        "🔔 Уведомления",
        "📩 Приход/Расход хабарлари",
        "⏳ Тасдиқлаш ҳолати",
        "⬅️ Орқага",
        "🔧 Механик менюси",
        "🧑‍🔍 Текширувчи менюси",
        "🚚 Ҳайдовчилар",
        "🔧 Механиклар",
        "⛽ Заправщик",
        "📋 Текширувдаги ходимлар",
        "👥 Ходимлар",
        "📊 Ҳисоботлар",
        "📋 Отчет Ремонт",
        "⛽ Отчет Дизел",
        "🟢 Отчет Газ",
        "📄 EXCEL файл",
        "📚 История Ремонт",
        "📋 Текшириш",
        "🚗 Ремонтга киритиш",
        "🚗 Ремонтдан чиқариш",
        "📜 Тарих",
        "🏠 Бош меню",
    ]:
        await clear_all_inline_messages(context, update.effective_chat.id)

    # === V76: Заправшик уведомления карточкасидан Орқага -> рўйхат ===
    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode in [
        "zapravshik_notif_prihod_return_card",
        "zapravshik_notif_diesel_rejected_card",
        "zapravshik_notif_prihod_pending_card",
        "zapravshik_notif_diesel_pending_card",
        "diesel_prihod_db_card",
        "diesel_prihod_db_edit_menu",
        "diesel_prihod_db_edit_firm",
        "diesel_prihod_db_edit_liter",
        "diesel_prihod_db_edit_note",
        "diesel_prihod_db_edit_video",
        "diesel_prihod_db_edit_photo",
        "dieselgive_edit_menu",
        "dieselgive_edit_firm",
        "dieselgive_edit_car",
        "dieselgive_edit_liter",
        "dieselgive_edit_note",
        "dieselgive_edit_speed_photo",
        "dieselgive_edit_video",
    ]:
        await clear_all_inline_messages(context, update.effective_chat.id)
        diesel_prihod_clear_staged(context)

        card_kind = context.user_data.get("zapravshik_notif_card_kind") or "rejected"
        user_id = update.effective_user.id

        # Вақтинчалик таҳрирлар базага сақланмайди.
        context.user_data.pop("diesel_edit_rejected_id", None)
        context.user_data.pop("dieselgive_firm", None)
        context.user_data.pop("dieselgive_liter", None)
        context.user_data.pop("dieselgive_note", None)
        context.user_data.pop("dieselgive_video_id", None)

        if card_kind == "pending":
            await show_zapravshik_pending_notifications_list(update.message, context, user_id)
        else:
            await show_zapravshik_rejected_notifications_list(update.message, context, user_id)
        return

    # === V70: BLOCK қилинган ходим ботдан фойдалана олмайди ===
    if get_user(update) is None:
        current_status = get_driver_status(update.effective_user.id)

        if current_status == "Рад этилди":
            context.user_data.clear()
            await update.message.reply_text(
                "⛔ Сиз блоклангансиз. Администратор PLAY қилмагунча ботдан фойдалана олмайсиз.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

    # === V69: Регистрация/ходим таҳририда фирма танлаш юқори приоритет ===
    # Фирма номи умумий меню handler'ига тушиб кетмаслиги учун бу блок юқорида туради.
    if mode in ["driver_firm", "driver_edit_firm", "driver_edit_firm_mechanic"]:
        if text not in FIRM_NAMES:
            await update.message.reply_text(
                "❌ Фирмани пастки менюдан танланг.",
                reply_markup=firm_keyboard()
            )
            return

        context.user_data["driver_firm"] = text

        if mode == "driver_edit_firm_mechanic":
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        if context.user_data.get("driver_work_role") == "mechanic":
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        next_mode = "driver_edit_car" if mode == "driver_edit_firm" else "driver_car"
        context.user_data["driver_car"] = ""
        context.user_data["mode"] = next_mode

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        await update.message.reply_text(
            f"🏢 Фирма: {text}\n\n🚛 Техникани танланг:",
            reply_markup=car_buttons_by_firm(text, only_available_for_driver=True)
        )
        return

    # === V68: Заправшик дизел уведомлениясида Орқага ===
    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode in [
        "zapravshik_diesel_notifications_rejected",
        "zapravshik_diesel_notifications_pending",
    ]:
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "zapravshik_diesel_notifications"
        await update.message.reply_text(
            "🔔 Дизел уведомления",
            reply_markup=zapravshik_diesel_notifications_keyboard()
        )
        return

    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode == "zapravshik_diesel_notifications":
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "zapravshik_diesel_menu"
        await update.message.reply_text("🟡 Дизел бўлими", reply_markup=zapravshik_diesel_menu_keyboard())
        return


    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode == "zapravshik_diesel_menu":
        context.user_data.clear()
        context.user_data["mode"] = "zapravshik_main"
        await update.message.reply_text(zapravka_info_text(), reply_markup=zapravshik_main_keyboard())
        return


    # === V67: Заправшик дизел расходида Орқага фақат 1 қадам ===
    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode in [
        "dieselgive_firm",
        "dieselgive_car",
        "dieselgive_edit_car",
    ]:
        context.user_data.clear()
        context.user_data["mode"] = "zapravshik_diesel_menu"
        await update.message.reply_text("🟡 Дизел бўлими", reply_markup=zapravshik_diesel_menu_keyboard())
        return

    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode == "dieselgive_liter":
        context.user_data["mode"] = "dieselgive_car"
        firm_name = context.user_data.get("dieselgive_firm")
        await update.message.reply_text(
            "🚛 Қайси дизел техникага ДИЗЕЛ беряпсиз?",
            reply_markup=diesel_cars_by_firm_keyboard(
                firm_name,
                context.user_data.get("dieselgive_from_car")
            )
        )
        return

    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode == "dieselgive_note":
        context.user_data["mode"] = "dieselgive_liter"
        await update.message.reply_text(
            "⛽ Неччи литр дизел беряпсиз?\n\nФақат сон киритинг.",
            reply_markup=back_keyboard()
        )
        return

    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode == "dieselgive_speed_photo":
        context.user_data["mode"] = "dieselgive_note"
        await update.message.reply_text(
            "📝 Изоҳ киритинг.",
            reply_markup=back_keyboard()
        )
        return

    if text == "⬅️ Орқага" and current_role == "zapravshik" and mode in [
        "dieselgive_confirm",
        "dieselgive_edit_menu",
    ]:
        context.user_data["mode"] = "dieselgive_video"
        await update.message.reply_text(
            "🎥 10 секунддан кам бўлмаган думалоқ видео юборинг.",
            reply_markup=back_keyboard()
        )
        return


    if text == "⬅️ Орқага" and current_role == "technadzor":
        await clear_all_inline_messages(context, update.effective_chat.id)

    if text == "⬅️ Орқага" and mode in [
        "diesel_prihod_db_edit_menu",
        "diesel_prihod_db_edit_firm",
        "diesel_prihod_db_edit_liter",
        "diesel_prihod_db_edit_note",
        "diesel_prihod_db_edit_video",
        "diesel_prihod_db_edit_photo",
    ]:
        await clear_all_inline_messages(context, update.effective_chat.id)

        diesel_prihod_clear_staged(context)

        record_id = context.user_data.get("diesel_prihod_editing_db_id") or context.user_data.get("diesel_prihod_current_id")

        if record_id:
            context.user_data["mode"] = "technadzor_diesel_prihod_card"

            if diesel_prihod_row_to_context(record_id, context):
                await show_diesel_prihod_db_card_after_edit(update.message, context, record_id)
                return

        context.user_data["mode"] = "technadzor_diesel_prihod_list"
        msg = await update.message.reply_text(
            "⛽ Текширувда турган дизел приходлар:",
            reply_markup=diesel_prihod_pending_keyboard()
        )
        remember_inline_message(context, msg)
        return

    if text == "⬅️ Орқага" and current_role == "technadzor" and mode == "technadzor_diesel_prihod_card":
        diesel_prihod_clear_staged(context)
        context.user_data["mode"] = "technadzor_diesel_prihod_list"
        msg = await update.message.reply_text(
            "⛽ Текширувда турган дизел приходлар:",
            reply_markup=diesel_prihod_pending_keyboard()
        )
        remember_inline_message(context, msg)
        return

    # === PRIORITY: Текширувчи ходим карточкаси/таҳриридан орқага ===
    # Ходимлар рўйхатидан очилган карточка -> айнан ўша рўйхатга қайтади.
    # Регистрациядан очилган текширувдаги карточка -> регистрация рўйхатига қайтади.
    if current_role == "technadzor" and text == "⬅️ Орқага" and mode in [
        "technadzor_staff_edit_name",
        "technadzor_staff_edit_surname",
        "technadzor_staff_edit_phone",
        "technadzor_staff_edit_role",
        "technadzor_staff_edit_firm",
        "technadzor_staff_edit_car",
        "technadzor_staff_card",
    ]:
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        staff = get_staff_by_id(driver_id) if driver_id else None

        await clear_technadzor_staff_inline(context, update.effective_chat.id)

        # Агар карточка оддий Ходимлар менюси рўйхатидан очилган бўлса,
        # пастки Орқага фақат битта орқага — ўша ходимлар рўйхатига қайтаради.
        if staff and staff.get("status") != "Текширувда":
            stack = context.user_data.get("technadzor_staff_action_stack", [])
            last_list = None
            while stack:
                item = stack.pop()
                if item.get("screen") == "list":
                    last_list = item
                    break
            context.user_data["technadzor_staff_action_stack"] = stack

            staff_type = (last_list or {}).get("staff_type") or context.user_data.get("technadzor_staff_type") or get_staff_type_from_work_role(staff.get("work_role"))
            firm = (last_list or {}).get("firm") if last_list is not None else context.user_data.get("technadzor_staff_firm")

            context.user_data["technadzor_staff_type"] = staff_type
            context.user_data["technadzor_staff_firm"] = firm
            context.user_data["mode"] = "technadzor_staff_drivers_list" if staff_type == "drivers" else f"technadzor_staff_{staff_type}_list"

            await update.message.reply_text(
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "Керакли ходимни танланг:",
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        # Текширувдаги регистрация карточкасида эски логика сақланади.
        if driver_id and technadzor_rollback_pending_edit(context, driver_id):
            context.user_data["mode"] = "technadzor_registration_list"
            await update.message.reply_text(
                "↩️ Таҳрирлаш бекор қилинди. Эски маълумотлар тикланди.",
                reply_markup=only_back_keyboard()
            )
            msg = await update.message.reply_text(
                "Текширувда турган ходимлар:",
                reply_markup=technadzor_pending_registration_keyboard()
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        context.user_data["mode"] = "technadzor_registration_list"
        await update.message.reply_text("📝 Регистрация текшируви", reply_markup=only_back_keyboard())
        msg = await update.message.reply_text(
            "Текширувда турган ходимлар:",
            reply_markup=technadzor_pending_registration_keyboard()
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return

    # === Заправщик: дизел приход text flow ===
    if current_role == "zapravshik":
        if text == "📊 Ҳисобот":
            await clear_all_inline_messages(context, update.effective_chat.id)
            context.user_data["mode"] = "zapravshik_report"
            wait_msg = await update.message.reply_text("⏳ Excel ҳисобот тайёрланяпти...")
            try:
                report_file = build_zapravshik_diesel_report_file()
                filename = f"diesel_hisobot_{datetime.now(ZoneInfo('Asia/Tashkent')).strftime('%Y_%m_%d_%H_%M')}.xlsx"
                await update.message.reply_document(
                    document=InputFile(report_file, filename=filename),
                    filename=filename,
                    caption=(
                        "📊 Ҳисобот тайёр.\n"
                        "Файл ичида 2 та лист бор:\n"
                        "1) Дизел Расходлар\n"
                        "2) Дизел Приходлар"
                    ),
                    reply_markup=zapravshik_main_keyboard()
                )
            except Exception as e:
                print("ZAPRAVSHIK DIESEL REPORT ERROR:", e)
                try:
                    conn.rollback()
                except Exception:
                    pass
                await update.message.reply_text(
                    "❌ Ҳисобот тайёрлашда хато бўлди. Илтимос, қайта уриниб кўринг.",
                    reply_markup=zapravshik_main_keyboard()
                )
            try:
                await wait_msg.delete()
            except Exception:
                pass
            return
        if text == "⬅️ Орқага" and mode == "zapravshik_diesel_menu":
            context.user_data.clear()
            await update.message.reply_text(zapravka_info_text(), reply_markup=zapravshik_main_keyboard())
            return

        if text == "⬅️ Орқага" and mode in [
            "other_diesel_liter",
            "other_diesel_note",
            "other_diesel_video",
            "other_diesel_confirm",
            "diesel_prihod_firm",
            "diesel_prihod_liter",
            "diesel_prihod_note",
            "diesel_prihod_video",
            "diesel_prihod_photo",
            "diesel_prihod_confirm",
            "diesel_prihod_edit_firm",
            "diesel_prihod_edit_liter",
            "diesel_prihod_edit_note",
            "diesel_prihod_edit_video",
            "diesel_prihod_edit_photo",
        ]:
            context.user_data["mode"] = "zapravshik_diesel_menu"
            await update.message.reply_text("🟡 Дизел бўлими", reply_markup=zapravshik_diesel_menu_keyboard())
            return

        if text == "🟡 Дизел бўлими":
            diesel_prihod_clear_staged(context)
            context.user_data.clear()
            context.user_data["mode"] = "zapravshik_diesel_menu"
            await update.message.reply_text("🟡 Дизел бўлими", reply_markup=zapravshik_diesel_menu_keyboard())
            return

        if text == "🔔 Уведомления":
            context.user_data.clear()
            context.user_data["mode"] = "zapravshik_diesel_notifications"
            await update.message.reply_text(
                "🔔 Дизел уведомления",
                reply_markup=zapravshik_diesel_notifications_keyboard()
            )
            return

        if text == "📩 Приход/Расход хабарлари":
            await show_zapravshik_rejected_notifications_list(update.message, context, update.effective_user.id)
            return

        if text == "⏳ Тасдиқлаш ҳолати":
            await show_zapravshik_pending_notifications_list(update.message, context, update.effective_user.id)
            return

        if text.startswith("📦 Бошқа дизел расходлар"):
            context.user_data.clear()
            context.user_data["mode"] = "other_diesel_liter"
            await update.message.reply_text(
                "⛽ Неччи литр расход қиляпсиз?\n\nФақат сон киритинг.",
                reply_markup=back_keyboard()
            )
            return

        if text == "➕ Дизел приход":
            context.user_data.clear()
            context.user_data["mode"] = "diesel_prihod_firm"
            await update.message.reply_text(
                "🏢 Қайси фирмага дизел приход қиляпсиз?",
                reply_markup=diesel_prihod_firm_stock_keyboard()
            )
            return

        if text == "➖ Дизел расход":
            context.user_data.clear()
            context.user_data["mode"] = "dieselgive_firm"
            context.user_data["dieselgive_from_car"] = "Заправщик"
            context.user_data["dieselgive_from_name"] = get_driver_full_name_by_telegram_id(update.effective_user.id)
            await update.message.reply_text(
                "🏢 Қайси фирмага дизел расход қиляпсиз?",
                reply_markup=diesel_prihod_firm_stock_keyboard()
            )
            return

    if mode == "other_diesel_liter":
        clean = text.replace(",", ".").strip()
        if not re.fullmatch(r"\d+(\.\d+)?", clean):
            await update.message.reply_text(
                "❌ Фақат сон киритинг.",
                reply_markup=back_keyboard()
            )
            return

        can_spend, total_stock, spend = can_spend_diesel_amount(clean)
        if not can_spend:
            await update.message.reply_text(
                "❌ Дизел остаткаси етарли эмас.\n\n"
                f"Жами фирмалар остаткаси: {format_liter(total_stock)} л\n"
                f"Сиз киритган расход: {format_liter(spend)} л\n\n"
                "📦 Бошқа дизел расходлар остаткаси бу ҳисобга қўшилмайди.",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["other_diesel_liter"] = clean
        context.user_data["mode"] = "other_diesel_note"
        await update.message.reply_text(
            "📝 Изоҳ киритинг:",
            reply_markup=back_keyboard()
        )
        return

    if mode == "other_diesel_note":
        if not re.search(r"[A-Za-zА-Яа-я0-9ЎўҚқҒғҲҳ]", text):
            await update.message.reply_text(
                "❌ Тўғри изоҳ киритинг.",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["other_diesel_note"] = text.strip()
        context.user_data["mode"] = "other_diesel_video"
        await update.message.reply_text(
            "🎥 Видео юборинг.",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["diesel_prihod_firm", "diesel_prihod_edit_firm"]:
        firm_name = extract_firm_from_stock_button(text)

        if firm_name not in FIRM_NAMES:
            await update.message.reply_text(
                "❌ Фирмани пастки менюдан танланг.",
                reply_markup=diesel_prihod_firm_stock_keyboard()
            )
            return

        context.user_data["diesel_prihod_firm"] = firm_name

        if mode == "diesel_prihod_edit_firm":
            context.user_data["mode"] = "diesel_prihod_confirm"
            await update.message.reply_text(
                diesel_prihod_card_text(context, status="------"),
                reply_markup=diesel_prihod_confirm_keyboard()
            )
            return

        context.user_data["mode"] = "diesel_prihod_liter"
        await update.message.reply_text(
            "⛽ Неччи литр приход қиляпсиз?\n\nФақат сон киритинг.\nМисол: 1500",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["diesel_prihod_liter", "diesel_prihod_edit_liter"]:
        if not is_valid_liter_amount(text):
            await update.message.reply_text(
                "❌ Нотўғри маълумот.\n\n⛽ Фақат сон киритинг.\nМисол: 1500",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["diesel_prihod_liter"] = text.strip()

        if mode == "diesel_prihod_edit_liter":
            context.user_data["mode"] = "diesel_prihod_confirm"
            await update.message.reply_text(diesel_prihod_card_text(context), reply_markup=diesel_prihod_confirm_keyboard())
            return

        context.user_data["mode"] = "diesel_prihod_note"
        await update.message.reply_text(
            "📝 Заявка номери ёки изоҳ киритинг.\n\nФақат текст ва рақам қабул қилинади.",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["diesel_prihod_note", "diesel_prihod_edit_note"]:
        if not is_valid_text_number_note(text):
            await update.message.reply_text(
                "❌ Нотўғри маълумот.\n\n📝 Фақат текст ва рақам киритинг.",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["diesel_prihod_note"] = text.strip()

        if mode == "diesel_prihod_edit_note":
            context.user_data["mode"] = "diesel_prihod_confirm"
            await update.message.reply_text(diesel_prihod_card_text(context), reply_markup=diesel_prihod_confirm_keyboard())
            return

        context.user_data["mode"] = "diesel_prihod_video"
        await update.message.reply_text(
            "🎥 Холат видеосини юборинг.\n\n"
            "Олиб келган техника номери ва бак қопқоғи очилиб, уровени кўринсин.\n"
            "Фақат 10 секунддан кам бўлмаган думалоқ видео қабул қилинади.",
            reply_markup=back_keyboard()
        )
        return

    if mode == "diesel_prihod_reject_note":
        record_id = context.user_data.get("diesel_prihod_reject_id")
        if not is_valid_text_number_note(text):
            await update.message.reply_text("❌ Нотўғри маълумот. Фақат текст ва рақам киритинг.", reply_markup=back_keyboard())
            return

        try:
            cursor.execute("""
                UPDATE diesel_prihod
                SET status = %s,
                    note = COALESCE(note, '') || %s
                WHERE id = %s
            """, ("Қайтди", "\nРад изоҳи: " + text.strip(), int(record_id)))
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD REJECT SAVE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_keyboard())
            return

        await update.message.reply_text("❌ Дизел приход рад этилди ва заправщикка қайтарилди.", reply_markup=technadzor_keyboard())
        await send_diesel_prihod_returned_to_sender(context, record_id, text.strip())
        context.user_data.clear()
        return

    if mode == "diesel_prihod_db_edit_firm":
        record_id = context.user_data.get("diesel_prihod_editing_db_id")
        firm_name = extract_firm_from_stock_button(text)

        if firm_name not in FIRM_NAMES:
            await update.message.reply_text(
                "❌ Фирмани пастки менюдан танланг.",
                reply_markup=diesel_prihod_firm_stock_keyboard()
            )
            return

        if not diesel_prihod_row_to_context(record_id, context):
            await update.message.reply_text("❌ Маълумот топилмади.", reply_markup=zapravshik_diesel_menu_keyboard())
            return

        context.user_data["diesel_prihod_firm"] = firm_name
        diesel_prihod_mark_staged(context, record_id)

        context.user_data["mode"] = "diesel_prihod_db_card"
        await show_diesel_prihod_staged_card(update.message, context, record_id)
        return

    if mode == "diesel_prihod_db_edit_liter":
        record_id = context.user_data.get("diesel_prihod_editing_db_id")
        if not is_valid_liter_amount(text):
            await update.message.reply_text("❌ Нотўғри маълумот. Фақат сон киритинг.", reply_markup=back_keyboard())
            return
        context.user_data["diesel_prihod_liter"] = text.strip()
        diesel_prihod_mark_staged(context, record_id)

        context.user_data["mode"] = "diesel_prihod_db_card"
        await show_diesel_prihod_staged_card(update.message, context, record_id)
        return

    if mode == "diesel_prihod_db_edit_note":
        record_id = context.user_data.get("diesel_prihod_editing_db_id")
        if not is_valid_text_number_note(text):
            await update.message.reply_text("❌ Нотўғри маълумот. Фақат текст ва рақам киритинг.", reply_markup=back_keyboard())
            return
        if not diesel_prihod_has_staged_for(context, record_id):
            diesel_prihod_row_to_context(record_id, context)

        context.user_data["diesel_prihod_note"] = text.strip()
        diesel_prihod_mark_staged(context, record_id)

        context.user_data["mode"] = "diesel_prihod_db_card"
        await show_diesel_prihod_staged_card(update.message, context, record_id)
        return

    if mode == "other_diesel_video":
        file_id = None

        if update.message.video_note:
            if (update.message.video_note.duration or 0) < 10:
                await update.message.reply_text(
                    "❌ Видео 10 секунддан кам бўлмасин.",
                    reply_markup=back_keyboard()
                )
                return
            file_id = update.message.video_note.file_id

        elif update.message.video:
            if (update.message.video.duration or 0) < 10:
                await update.message.reply_text(
                    "❌ Видео 10 секунддан кам бўлмасин.",
                    reply_markup=back_keyboard()
                )
                return
            file_id = update.message.video.file_id

        else:
            await update.message.reply_text(
                "❌ Фақат видео юборинг.",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["other_diesel_video_id"] = file_id
        context.user_data["mode"] = "other_diesel_confirm"

        context.user_data["other_diesel_telegram_id"] = update.effective_user.id
        context.user_data["other_diesel_time"] = now_text()

        await update.message.reply_text(
            other_diesel_card_text(context, status="------"),
            reply_markup=other_diesel_confirm_keyboard(has_media=bool(context.user_data.get('other_diesel_video_id')))
        )
        return

    if mode in ["diesel_prihod_video", "diesel_prihod_edit_video", "diesel_prihod_db_edit_video"]:
        await update.message.reply_text(
            "❌ Бу босқичда фақат 10 секунддан кам бўлмаган думалоқ видео қабул қилинади.",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["diesel_prihod_photo", "diesel_prihod_edit_photo", "diesel_prihod_db_edit_photo"]:
        await update.message.reply_text(
            "❌ Бу босқичда фақат накладной расми қабул қилинади.",
            reply_markup=back_keyboard()
        )
        return

    # === PRIORITY: Регистрация тасдиқ/раддан кейин пастки Орқага ===
    # Бу блок барча умумий back handler'лардан олдин ишлайди.
    if text == "⬅️ Орқага" and mode == "technadzor_registration_after_decision" and current_role == "technadzor":
        await clear_technadzor_staff_inline(context, update.effective_chat.id)

        context.user_data.pop("operation", None)
        context.user_data.pop("firm", None)
        context.user_data.pop("car", None)
        context.user_data.pop("history_car", None)
        context.user_data.pop("technadzor_selected_staff_id", None)
        context.user_data.pop("technadzor_staff_action_stack", None)

        pending_count = pending_registration_count()

        if pending_count > 0:
            context.user_data["mode"] = "technadzor_registration_list"
            await update.message.reply_text(
                "📝 Регистрация текшируви",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "Текширувда турган ходимлар:",
                reply_markup=technadzor_pending_registration_keyboard()
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        context.user_data["mode"] = "technadzor_staff_menu"
        await update.message.reply_text(
            "👥 Ходимлар менюси",
            reply_markup=technadzor_staff_menu_keyboard()
        )
        return

    # === Текширувчи янги меню структураси ===
    if current_role == "technadzor":
        if text == "⬅️ Орқага" and mode in [
            "technadzor_notifications_menu",
            "select_firm_for_add",
            "technadzor_staff_menu",
            "technadzor_history_menu",
            "technadzor_reports_menu",
        ]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data.clear()
            await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
            return

        if text == "⬅️ Орқага" and mode in [
            "technadzor_registration_list",
            "confirm_exit",
            "confirm_exit_card",
            "technadzor_diesel_prihod_list",
            "history_select_firm",
            "history_select_car",
            "history_period",
            "technadzor_report_remont_menu",
            "technadzor_report_diesel_menu",
            "technadzor_diesel_history_role",
            "technadzor_diesel_history_zap_period",
            "technadzor_diesel_history_zap_custom",
            "technadzor_diesel_history_zap_list",
        ]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            if mode == "confirm_exit_card":
                context.user_data["mode"] = "confirm_exit"
                await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=only_back_keyboard())
                msg = await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
                remember_inline_message(context, msg)
                return

            if mode in ["technadzor_registration_list", "confirm_exit", "technadzor_diesel_prihod_list"]:
                context.user_data["mode"] = "technadzor_notifications_menu"
                await update.message.reply_text("🔔 Уведомления", reply_markup=technadzor_notifications_keyboard())
                return

            if mode == "history_period":
                firm = context.user_data.get("firm")
                context.user_data.pop("history_car", None)
                context.user_data["mode"] = "history_select_car"

                await clear_all_inline_messages(context, update.effective_chat.id)
                await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=back_keyboard())
                if firm:
                    msg = await update.message.reply_text("Техникани танланг:", reply_markup=history_car_buttons_by_firm(firm))
                    remember_inline_message(context, msg)
                else:
                    context.user_data["mode"] = "history_select_firm"
                    await update.message.reply_text("Қайси фирма техникасини кўрмоқчисиз?", reply_markup=firm_back_keyboard())
                return

            if context.user_data.get("history_parent_menu") == "technadzor_report_remont_menu":
                context.user_data.pop("history_parent_menu", None)
                context.user_data.pop("history_car", None)
                context.user_data["mode"] = "technadzor_report_remont_menu"
                await update.message.reply_text("📋 Отчет Ремонт", reply_markup=technadzor_remont_report_keyboard())
                return

            if mode in ["technadzor_diesel_history_zap_custom", "technadzor_diesel_history_zap_list"]:
                context.user_data["mode"] = "technadzor_diesel_history_zap_period"
                await update.message.reply_text("⛽ Заправщик дизел тарихи\n\nДаврни танланг:", reply_markup=technadzor_diesel_history_period_keyboard())
                return

            if mode in ["technadzor_diesel_history_driver_custom", "technadzor_diesel_history_driver_list"]:
                context.user_data["mode"] = "technadzor_diesel_history_driver_period"
                await update.message.reply_text("🚛 Ҳайдовчи дизел тарихи\n\nДаврни танланг:", reply_markup=technadzor_diesel_history_period_keyboard())
                return

            if mode in ["technadzor_diesel_history_zap_period", "technadzor_diesel_history_driver_period"]:
                context.user_data["mode"] = "technadzor_diesel_history_role"
                await update.message.reply_text("📚 История Дизел\n\nҚайси бўлимни кўрмоқчисиз?", reply_markup=technadzor_diesel_history_role_keyboard())
                return

            if mode == "technadzor_diesel_history_role":
                context.user_data["mode"] = "technadzor_report_diesel_menu"
                await update.message.reply_text("⛽ Отчет Дизел", reply_markup=technadzor_diesel_report_keyboard())
                return

            if mode in ["technadzor_report_remont_menu", "technadzor_report_diesel_menu"]:
                context.user_data["mode"] = "technadzor_reports_menu"
                await update.message.reply_text("📊 Ҳисоботлар менюси", reply_markup=technadzor_reports_keyboard())
                return

            context.user_data["mode"] = "technadzor_history_menu"
            await update.message.reply_text("💾 История", reply_markup=technadzor_history_keyboard())
            return

        if text.startswith("🔔 Уведомления"):
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data.clear()
            context.user_data["mode"] = "technadzor_notifications_menu"
            await update.message.reply_text("🔔 Уведомления", reply_markup=technadzor_notifications_keyboard())
            return

        if mode == "technadzor_notifications_menu":
            if text.startswith("📝 Регистрация"):
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_registration_list"
                context.user_data["technadzor_staff_action_stack"] = []
                await update.message.reply_text("📝 Регистрация текшируви", reply_markup=only_back_keyboard())
                msg = await update.message.reply_text(
                    "Текширувда турган ходимлар:",
                    reply_markup=technadzor_pending_registration_keyboard()
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            if text.startswith("🛠 Ремонтдан чиқариш"):
                context.user_data["mode"] = "confirm_exit"
                await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=only_back_keyboard())
                msg = await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
                remember_inline_message(context, msg)
                return

            if text.startswith("⛽ Дизел приход"):
                context.user_data["mode"] = "technadzor_diesel_prihod_list"
                await update.message.reply_text("⛽ Текширувда турган дизел приходлар:", reply_markup=only_back_keyboard())
                msg = await update.message.reply_text("⛽ Текширувда турган дизел приходлар:", reply_markup=diesel_prihod_pending_keyboard())
                remember_inline_message(context, msg)
                return

        if text == "🔧 Ремонтга қўшиш":
            context.user_data.clear()
            context.user_data["mode"] = "select_firm_for_add"
            await update.message.reply_text("🔴 <b>Фирмани танланг:</b>", parse_mode="HTML", reply_markup=firm_back_keyboard())
            return

        if text == "👥 Ходимлар":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_menu"
            await update.message.reply_text("👥 Ходимлар менюси", reply_markup=technadzor_staff_menu_keyboard())
            return

        if text == "💾 История":
            context.user_data["mode"] = "technadzor_history_menu"
            await update.message.reply_text("💾 История", reply_markup=technadzor_history_keyboard())
            return

        if text == "📊 Ҳисоботлар":
            await clear_all_inline_messages(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_reports_menu"
            await update.message.reply_text("📊 Ҳисоботлар менюси", reply_markup=technadzor_reports_keyboard())
            return

        if mode == "technadzor_reports_menu":
            if text == "📋 Отчет Ремонт":
                context.user_data["mode"] = "technadzor_report_remont_menu"
                await update.message.reply_text("📋 Отчет Ремонт", reply_markup=technadzor_remont_report_keyboard())
                return

            if text == "⛽ Отчет Дизел":
                context.user_data["mode"] = "technadzor_report_diesel_menu"
                await update.message.reply_text("⛽ Отчет Дизел", reply_markup=technadzor_diesel_report_keyboard())
                return

            if text == "🟢 Отчет Газ":
                await update.message.reply_text("🟢 Отчет Газ кейинги босқичда қўшилади.", reply_markup=technadzor_reports_keyboard())
                return

        if mode == "technadzor_report_remont_menu":
            if text == "📄 EXCEL файл":
                wait_msg = await update.message.reply_text("⏳ Ремонт Excel ҳисоботи тайёрланяпти...")
                try:
                    report_file = build_technadzor_remont_report_file()
                    filename = f"remont_hisobot_{datetime.now(ZoneInfo('Asia/Tashkent')).strftime('%Y_%m_%d_%H_%M')}.xlsx"
                    await update.message.reply_document(
                        document=InputFile(report_file, filename=filename),
                        filename=filename,
                        caption="📋 Отчет Ремонт тайёр.\nМеханик ва текширувчи ID ўрнига Фамилия Исми чиқади.",
                        reply_markup=technadzor_remont_report_keyboard()
                    )
                except Exception as e:
                    print("TECHNADZOR REMONT EXCEL REPORT ERROR:", e)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    await update.message.reply_text(
                        "❌ Отчет Ремонт Excel файлини тайёрлашда хато бўлди. Илтимос, қайта уриниб кўринг.",
                        reply_markup=technadzor_remont_report_keyboard()
                    )
                try:
                    await wait_msg.delete()
                except Exception:
                    pass
                return

            if text == "📚 История Ремонт":
                await clear_all_inline_messages(context, update.effective_chat.id)
                context.user_data["mode"] = "history_select_firm"
                context.user_data["history_parent_menu"] = "technadzor_report_remont_menu"
                await update.message.reply_text("Қайси фирма техникасини кўрмоқчисиз?", reply_markup=firm_back_keyboard())
                return

        if mode == "technadzor_report_diesel_menu":
            if text == "📄 EXCEL файл":
                wait_msg = await update.message.reply_text("⏳ Дизел Excel ҳисоботи тайёрланяпти...")
                try:
                    report_file = build_technadzor_diesel_report_file()
                    filename = f"diesel_hisobot_{datetime.now(ZoneInfo('Asia/Tashkent')).strftime('%Y_%m_%d_%H_%M')}.xlsx"
                    await update.message.reply_document(
                        document=InputFile(report_file, filename=filename),
                        filename=filename,
                        caption="⛽ Отчет Дизел тайёр.\nTelegram ID ўрнига Фамилия Исми чиқади.",
                        reply_markup=technadzor_diesel_report_keyboard()
                    )
                except Exception as e:
                    print("TECHNADZOR DIESEL EXCEL REPORT ERROR:", e)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    await update.message.reply_text(
                        "❌ Отчет Дизел Excel файлини тайёрлашда хато бўлди. Илтимос, қайта уриниб кўринг.",
                        reply_markup=technadzor_diesel_report_keyboard()
                    )
                try:
                    await wait_msg.delete()
                except Exception:
                    pass
                return

            if text == "📚 История Дизел":
                await clear_all_inline_messages(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_diesel_history_role"
                await update.message.reply_text(
                    "📚 История Дизел\n\nҚайси бўлимни кўрмоқчисиз?",
                    reply_markup=technadzor_diesel_history_role_keyboard()
                )
                return

        if mode == "technadzor_diesel_history_role":
            if text == "⛽ Заправщик":
                context.user_data["mode"] = "technadzor_diesel_history_zap_period"
                context.user_data["technadzor_diesel_history_target"] = "zapravshik"
                await update.message.reply_text(
                    "⛽ Заправщик дизел тарихи\n\nДаврни танланг:",
                    reply_markup=technadzor_diesel_history_period_keyboard()
                )
                return

            if text == "🚛 Ҳайдовчи":
                context.user_data["mode"] = "technadzor_diesel_history_driver_period"
                context.user_data["technadzor_diesel_history_target"] = "driver"
                await update.message.reply_text(
                    "🚛 Ҳайдовчи дизел тарихи\n\nДаврни танланг:",
                    reply_markup=technadzor_diesel_history_period_keyboard()
                )
                return

        if mode == "technadzor_diesel_history_zap_period":
            period_map = {
                "📅 1 кунлик": "1d",
                "📅 10 кунлик": "10d",
                "📅 1 ойлик": "30d",
            }

            if text in period_map:
                start_date, end_date = get_technadzor_diesel_history_dates(period_map[text])
                if not start_date or not end_date:
                    await update.message.reply_text("❌ Давр хатолик.", reply_markup=technadzor_diesel_history_period_keyboard())
                    return

                context.user_data["mode"] = "technadzor_diesel_history_zap_list"
                context.user_data["technadzor_diesel_history_start"] = start_date
                context.user_data["technadzor_diesel_history_end"] = end_date
                title = (
                    "⛽ Заправщик Дизел тарихи\n"
                    f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                    "Рўйхат тартиби:\nТехника номери / Фирма / Литр"
                )
                await update.message.reply_text(
                    "✅ Давр танланди. Қуйидаги inline рўйхатдан маълумотни танланг:",
                    reply_markup=ReplyKeyboardRemove()
                )
                msg = await update.message.reply_text(
                    title,
                    reply_markup=technadzor_zapravshik_diesel_history_list_keyboard(start_date, end_date)
                )
                remember_inline_message(context, msg)
                return

            if text == "📆 Санадан-санагача":
                context.user_data["mode"] = "technadzor_diesel_history_zap_custom"
                await update.message.reply_text(
                    "📆 Санадан-санагача даврни киритинг.\n\n"
                    "Мисол: 01.05.2026-15.05.2026",
                    reply_markup=only_back_keyboard()
                )
                return

        if mode == "technadzor_diesel_history_driver_period":
            period_map = {
                "📅 1 кунлик": "1d",
                "📅 10 кунлик": "10d",
                "📅 1 ойлик": "30d",
            }

            if text in period_map:
                start_date, end_date = get_technadzor_diesel_history_dates(period_map[text])
                if not start_date or not end_date:
                    await update.message.reply_text("❌ Давр хатолик.", reply_markup=technadzor_diesel_history_period_keyboard())
                    return

                context.user_data["mode"] = "technadzor_diesel_history_driver_list"
                context.user_data["technadzor_diesel_history_start"] = start_date
                context.user_data["technadzor_diesel_history_end"] = end_date
                context.user_data["technadzor_diesel_history_target"] = "driver"
                title = (
                    "🚛 Ҳайдовчи Дизел тарихи\n"
                    f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                    "Рўйхат тартиби:\nТехника номери / Фирма / Литр"
                )
                await update.message.reply_text(
                    "✅ Давр танланди. Қуйидаги inline рўйхатдан маълумотни танланг:",
                    reply_markup=ReplyKeyboardRemove()
                )
                msg = await update.message.reply_text(
                    title,
                    reply_markup=technadzor_driver_diesel_history_list_keyboard(start_date, end_date)
                )
                remember_inline_message(context, msg)
                return

            if text == "📆 Санадан-санагача":
                context.user_data["mode"] = "technadzor_diesel_history_driver_custom"
                await update.message.reply_text(
                    "📆 Санадан-санагача даврни киритинг.\n\n"
                    "Мисол: 01.05.2026-15.05.2026",
                    reply_markup=only_back_keyboard()
                )
                return

        if mode == "technadzor_diesel_history_driver_custom":
            start_date, end_date = parse_technadzor_custom_period(text)
            if not start_date or not end_date:
                await update.message.reply_text(
                    "❌ Нотўғри формат.\n\nМисол: 01.05.2026-15.05.2026",
                    reply_markup=only_back_keyboard()
                )
                return

            context.user_data["mode"] = "technadzor_diesel_history_driver_list"
            context.user_data["technadzor_diesel_history_start"] = start_date
            context.user_data["technadzor_diesel_history_end"] = end_date
            context.user_data["technadzor_diesel_history_target"] = "driver"
            await update.message.reply_text(
                "✅ Давр танланди. Қуйидаги inline рўйхатдан маълумотни танланг:",
                reply_markup=ReplyKeyboardRemove()
            )
            msg = await update.message.reply_text(
                "🚛 Ҳайдовчи Дизел тарихи\n"
                f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                "Рўйхат тартиби:\nТехника номери / Фирма / Литр",
                reply_markup=technadzor_driver_diesel_history_list_keyboard(start_date, end_date)
            )
            remember_inline_message(context, msg)
            return

        if mode == "technadzor_diesel_history_zap_custom":
            start_date, end_date = parse_technadzor_custom_period(text)
            if not start_date or not end_date:
                await update.message.reply_text(
                    "❌ Нотўғри формат.\n\nМисол: 01.05.2026-15.05.2026",
                    reply_markup=only_back_keyboard()
                )
                return

            context.user_data["mode"] = "technadzor_diesel_history_zap_list"
            context.user_data["technadzor_diesel_history_start"] = start_date
            context.user_data["technadzor_diesel_history_end"] = end_date
            msg = await update.message.reply_text(
                "⛽ Заправщик Дизел тарихи\n"
                f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                "Рўйхат тартиби:\nТехника номери / Фирма / Литр",
                reply_markup=technadzor_zapravshik_diesel_history_list_keyboard(start_date, end_date)
            )
            remember_inline_message(context, msg)
            return

        if mode == "technadzor_history_menu":
            if text == "⛽ История ГАЗ":
                await update.message.reply_text("⛽ История ГАЗ кейин ишлаб чиқилади.", reply_markup=technadzor_history_keyboard())
                return
            if text == "🟡 История Дизел":
                await update.message.reply_text("🟡 История Дизел кейин ишлаб чиқилади.", reply_markup=technadzor_history_keyboard())
                return

    if mode == "driver_edit_role":
        role_map = {
            "🚚 Ҳайдовчи": "driver",
            "🔧 Механик": "mechanic",
            "⛽ Заправщик": "zapravshik",
        }

        if text not in role_map:
            await update.message.reply_text(
                "❌ Лавозимни пастки менюдан танланг.",
                reply_markup=register_role_keyboard()
            )
            return

        new_role = role_map[text]
        context.user_data["driver_work_role"] = new_role

        if new_role == "zapravshik":
            context.user_data["driver_firm"] = ""
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        context.user_data["driver_firm"] = ""
        context.user_data["driver_car"] = ""
        context.user_data["mode"] = "driver_edit_firm_mechanic" if new_role == "mechanic" else "driver_edit_firm"

        await update.message.reply_text(
            "🏢 Янги фирмани танланг:",
            reply_markup=firm_keyboard()
        )
        return

    if mode == "diesel_receive_reject_note":
        if not is_valid_gas_note(text):
            await update.message.reply_text("❌ Сабаб нотўғри. Фақат ҳарф ва рақам киритинг.")
            return

        transfer_id = context.user_data.get("diesel_reject_transfer_id")

        if not transfer_id:
            await update.message.reply_text("❌ Рад этилаётган дизел маълумоти топилмади.")
            context.user_data.clear()
            return

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                receiver_comment = %s,
                answered_at = NOW()
            WHERE id = %s
            RETURNING from_driver_id
        """, ("Рад этилди", text, transfer_id))

        row = cursor.fetchone()
        conn.commit()

        if not row:
            await update.message.reply_text("❌ Дизел маълумоти базадан топилмади.")
            context.user_data.clear()
            return

        await notify_diesel_sender_rejected(context, transfer_id, text)

        context.user_data.clear()

        await update.message.reply_text("❌ Маълумот рад этилди.")
        return
    
        transfer_id = context.user_data.get("diesel_reject_transfer_id")

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                receiver_comment = %s,
                answered_at = NOW()
            WHERE id = %s
            RETURNING from_driver_id, to_car, liter
        """, ("Рад этилди", text, transfer_id))

        row = cursor.fetchone()
        conn.commit()

        if not row:
            await update.message.reply_text("❌ Маълумот топилмади.")
            context.user_data.clear()
            return
        
        await notify_diesel_sender_rejected(context, transfer_id, text)
        
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)
        
        context.user_data.clear()
        
        await update.message.reply_text("❌ Дизел олиш рад этилди.")  
        await update.message.reply_text(
            driver_menu_text(update.effective_user.id),
            reply_markup=driver_main_keyboard(fuel_type)
        )
        
        return
        
        fuel_type = get_car_fuel_type(driver_car)

        context.user_data.clear()

        await update.message.reply_text("❌ Дизел олиш рад этилди.")
        await update.message.reply_text(
            driver_menu_text(update.effective_user.id),
            reply_markup=driver_main_keyboard(fuel_type)
        )
        return

    if mode == "choose_work_role":
        role_map = {
            "🚚 Ҳайдовчи": "driver",
            "🔧 Механик": "mechanic",
            "⛽ Заправщик": "zapravshik",
        }

        if text not in role_map:
            await update.message.reply_text(
                "❌ Қуйидаги тугмалардан бирини танланг:",
                reply_markup=register_role_keyboard()
            )
            return

        context.user_data["inline_disabled_by_start"] = False
        context.user_data["driver_work_role"] = role_map[text]
        context.user_data["mode"] = "driver_name"

        await update.message.reply_text(
            "🔴 <b>Исмингизни киритинг</b>\n\nМисол: Тешавой",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "dieselgive_note":

        if not is_valid_gas_note(text):
            await update.message.reply_text(
                "❌ Изоҳ нотўғри.\n\n"
                "📝 Фақат ҳарф ва рақамдан фойдаланинг.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["dieselgive_note"] = text
        context.user_data["mode"] = "dieselgive_speed_photo"

        await update.message.reply_text(
            "📍 Спидометр ёки моточас кўрсаткичини ёзинг.\n\n"
            "❌ Фақат сон қабул қилинади.\n"
            "Мисол: 125000",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode in ["dieselgive_speed_photo", "dieselgive_edit_speed_photo"]:
        clean_text = str(text or "").strip().replace(" ", "")
        if not clean_text.isdigit() or not (1 <= len(clean_text) <= 8):
            await update.message.reply_text(
                "❌ Нотўғри маълумот киритилди.\n\n"
                "📍 Спидометр ёки моточас кўрсаткичини фақат сон билан ёзинг.\n"
                "Мисол: 125000",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["dieselgive_speedometer_photo_id"] = clean_text

        if mode == "dieselgive_edit_speed_photo":
            context.user_data["mode"] = "dieselgive_confirm"
            await update.message.reply_text(
                diesel_confirm_text(context),
                reply_markup=diesel_give_final_keyboard()
            )
            return

        context.user_data["mode"] = "dieselgive_video"
        await update.message.reply_text(
            "🎥 Олди-берди қилаётган техникаларни думалоқ видео қилиб юборинг.\n\n"
            "⏱ Видео 10 сониядан кам бўлмасин.\n"
            "❌ Фақат думалоқ видео қабул қилинади.",
            reply_markup=ReplyKeyboardRemove()
        )
        return


# === DIESEL RECEIVER REJECT MESSAGE HANDLER END ===


    if mode == "gasgive_receiver_reject_note":
        if (
            update.message.photo
            or update.message.video
            or update.message.video_note
            or update.message.audio
            or update.message.voice
            or update.message.document
            or update.message.sticker
            or not is_valid_gas_note(text)
        ):
            await update.message.reply_text(
                "❌ Нотўғри маълумот киритилди.\n\n"
                "📝 Рад этиш сабабини фақат ҳарф ва рақам билан ёзинг."
            )
            return

        transfer_id = context.user_data.get("gasgive_reject_transfer_id")
        reject_note = text
        reject_time = now_text()

        cursor.execute("""
            UPDATE gas_transfers
            SET status = %s,
                receiver_comment = %s,
                answered_at = NOW()
            WHERE id = %s
            RETURNING from_driver_id, to_car
        """, ("Рад этилди", reject_note, transfer_id))

        row = cursor.fetchone()
        conn.commit()

        if row:
            await notify_gas_sender_rejected(context, transfer_id, reject_note)

        await update.message.reply_text(
            "❌ Маълумот рад этилди ва газ берувчи ҳайдовчига хабар юборилди."
        )

        context.user_data.clear()
        return

    if mode == "gasgive_edit_note_text":
        if not is_valid_gas_note(text):
            await update.message.reply_text(
                "❌ Изоҳ фақат ҳарф ва рақамдан иборат бўлиши керак.\n\n"
                "🔴 Янги изоҳни киритинг."
            )
            return

        context.user_data["gasgive_note"] = text
        context.user_data["mode"] = "gasgive_confirm"

        from_car = context.user_data.get("gasgive_from_car")
        to_car = context.user_data.get("gasgive_to_car")
        note = context.user_data.get("gasgive_note")
        created_time = context.user_data.get("gasgive_created_time") or now_text()

        sent_msg = await update.message.reply_text(
            gas_confirm_text(context),
            reply_markup=gas_give_confirm_keyboard()
        )

        context.user_data["gasgive_confirm_message_id"] = sent_msg.message_id


        schedule_gas_auto_confirm_task(context, update.effective_user.id)

        return

    if mode == "gasgive_note":
        if (
            update.message.photo
            or update.message.video
            or update.message.audio
            or update.message.voice
            or update.message.document
            or update.message.sticker
        ):
            await update.message.reply_text(
                "❌ Нотўғри маълумот киритилди.\n\n"
                "📝 Фақат текст ва рақам киритинг."
            )
            return

    if text == "⬅️ Орқага" and mode in ["driver_reports_received", "driver_reports_given", "driver_reports_gas_received", "driver_reports_gas_given"]:
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "driver_reports_menu"
        await update.message.reply_text(
            "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:",
            reply_markup=driver_reports_keyboard(update.effective_user.id)
        )
        return

    if text == "⬅️ Орқага" and mode == "driver_reports_menu":
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data.clear()
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)
        await update.message.reply_text(
            driver_menu_text(update.effective_user.id),
            reply_markup=driver_main_keyboard(fuel_type)
        )
        return

    if text == "⬅️ Орқага" and mode in ["diesel_receive_select", "diesel_receive_reject_note", "diesel_give_status"]:
        context.user_data["mode"] = "diesel_menu"

        await update.message.reply_text(
            "⛽ Дизел ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=diesel_report_keyboard(update.effective_user.id)
        )
        return

    if text == "⬅️ Орқага" and mode in ["fuel_menu", "gasgive_firm", "gasgive_car", "gasgive_note", "gasgive_video", "gasgive_confirm"]:
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if mode == "fuel_menu":
            context.user_data.clear()
            await update.message.reply_text(
                driver_menu_text(update.effective_user.id),
                reply_markup=driver_main_keyboard(fuel_type)
            )
            return

        if mode == "gasgive_firm":
            context.user_data["mode"] = "fuel_menu"
            await update.message.reply_text(
                "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
                reply_markup=gas_report_keyboard()
            )
            return

        if mode == "gasgive_car":
            context.user_data["mode"] = "gasgive_firm"
            await update.message.reply_text(
                "🏢 Қайси фирмадаги газли техникага ГАЗ беряпсиз?",
                reply_markup=gas_firm_keyboard()
            )
            return

        if mode in ["gasgive_note", "gasgive_video", "gasgive_confirm"]:
            context.user_data["mode"] = "gasgive_car"
            firm = context.user_data.get("gasgive_firm")
            await update.message.reply_text(
                "🚛 Қайси газли техникага ГАЗ беряпсиз?",
                reply_markup=gas_cars_by_firm_keyboard(
                    firm,
                    context.user_data.get("gasgive_from_car")
                )
            )
            return

    if mode == "fuel_gas_video":
        await update.message.reply_text(
            "❌ Бу босқичда матн қабул қилинмайди.\n\n"
            "🎥 Фақат видео юборинг."
        )
        return

    if mode == "fuel_gas_photo":
        await update.message.reply_text(
            "❌ Бу босқичда матн қабул қилинмайди.\n\n"
            "📷 Фақат ведомость расмини юборинг."
        )
        return

    if text == "📊 Ҳисоботлар":
        if current_role != "driver":
            await update.message.reply_text("❌ Бу меню фақат ҳайдовчи роли учун.")
            return

        context.user_data["mode"] = "driver_reports_menu"
        await update.message.reply_text(
            "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:",
            reply_markup=driver_reports_keyboard(update.effective_user.id)
        )
        return

    if text.startswith("📥 Приход Дизел маълумотлари"):
        if current_role != "driver":
            await update.message.reply_text("❌ Бу меню фақат ҳайдовчи роли учун.")
            return

        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "driver_reports_received"
        received_count, _ = get_driver_diesel_report_counts(update.effective_user.id)
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        msg = await update.message.reply_text(
            f"📥 Приход Дизел маълумотлари ({received_count} та)\n\n"
            "Статуси тасдиқланган дизел приход карточкалари:",
            reply_markup=driver_diesel_report_list_keyboard(update.effective_user.id, "received")
        )
        remember_inline_message(context, msg)
        return

    if text.startswith("📤 Расход Дизел маълумотлари"):
        if current_role != "driver":
            await update.message.reply_text("❌ Бу меню фақат ҳайдовчи роли учун.")
            return

        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "driver_reports_given"
        _, given_count = get_driver_diesel_report_counts(update.effective_user.id)
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        msg = await update.message.reply_text(
            f"📤 Расход Дизел маълумотлари ({given_count} та)\n\n"
            "Статуси тасдиқланган дизел расход карточкалари:",
            reply_markup=driver_diesel_report_list_keyboard(update.effective_user.id, "given")
        )
        remember_inline_message(context, msg)
        return

    if text.startswith("📥 Приход ГАЗ маълумотлари"):
        if current_role != "driver":
            await update.message.reply_text("❌ Бу меню фақат ҳайдовчи роли учун.")
            return

        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "driver_reports_gas_received"
        received_count, _ = get_driver_gas_report_counts(update.effective_user.id)
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        msg = await update.message.reply_text(
            f"📥 Приход ГАЗ маълумотлари ({received_count} та)\n\n"
            "Статуси тасдиқланган газ приход карточкалари:",
            reply_markup=driver_gas_report_list_keyboard(update.effective_user.id, "received")
        )
        remember_inline_message(context, msg)
        return

    if text.startswith("📤 Расход ГАЗ маълумотлари"):
        if current_role != "driver":
            await update.message.reply_text("❌ Бу меню фақат ҳайдовчи роли учун.")
            return

        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "driver_reports_gas_given"
        _, given_count = get_driver_gas_report_counts(update.effective_user.id)
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        msg = await update.message.reply_text(
            f"📤 Расход ГАЗ маълумотлари ({given_count} та)\n\n"
            "Статуси тасдиқланган газ расход карточкалари:",
            reply_markup=driver_gas_report_list_keyboard(update.effective_user.id, "given")
        )
        remember_inline_message(context, msg)
        return

    if text == "⛽ Ёқилғи ҳисоботи":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)
    
        if fuel_type.lower() == "газ":
            context.user_data["mode"] = "fuel_menu"
            context.user_data["fuel_type"] = "Газ"
    
            await update.message.reply_text(
                "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
                reply_markup=gas_report_keyboard()
            )
            return
    
        if fuel_type.lower() == "дизел":
            context.user_data["mode"] = "diesel_menu"
            context.user_data["fuel_type"] = "Дизел"
    
            await update.message.reply_text(
                "⛽ Дизел ҳисоботи бўлими\n\nАмални танланг:",
                reply_markup=diesel_report_keyboard(update.effective_user.id)
            )
            return
    
        await update.message.reply_text("❌ Бу техника учун ёқилғи тури топилмади.")
        return

        context.user_data["mode"] = "fuel_menu"

        await update.message.reply_text(
            "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=gas_report_keyboard()
        )
        

    if text == "⛽ ГАЗ бериш":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if fuel_type.lower() != "газ":
            await update.message.reply_text("❌ Бу бўлим фақат газли техникалар учун.")
            return

        context.user_data["mode"] = "gasgive_firm"
        context.user_data["gasgive_from_car"] = driver_car
        context.user_data["fuel_type"] = fuel_type

        await update.message.reply_text(
            "🏢 Қайси фирмадаги газли техникага ГАЗ беряпсиз?",
            reply_markup=gas_firm_keyboard()
        )
        return

    if text == "⛽ ГАЗ олиш":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if fuel_type.lower() != "газ":
            await update.message.reply_text("Бу бўлим фақат газли техникалар учун.")
            return

        context.user_data["mode"] = "fuel_gas_km"
        context.user_data["fuel_car"] = driver_car
        context.user_data["fuel_type"] = fuel_type

        await update.message.reply_text(
            "⛽ Газ ёқилғи ҳисоботи\n\n"
            "🔴 Спидометр кўрсаткичини киритинг (КМ)\n\n"
            "Мисол: 15300",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if text in ["✅ ДИЗЕЛ приход", "✅ ДИЗЕЛ олишни тасдиқлаш"]:
        driver_car = get_driver_car(update.effective_user.id)

        if not driver_car:
            await update.message.reply_text("❌ Сизга бириктирилган техника топилмади.")
            return

        context.user_data["mode"] = "diesel_receive_select"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        msg = await update.message.reply_text(
            "⛽ Ёқилғи ҳисоботи / ДИЗЕЛ приход\n\n"
            "Тасдиқлашда турган маълумотлар:",
            reply_markup=diesel_pending_confirm_keyboard(driver_car)
        )
        remember_inline_message(context, msg)
        return

    if text.startswith("📌 ДИЗЕЛ бериш статуси"):
        work_role = get_driver_work_role(update.effective_user.id)
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if work_role != "zapravshik" and fuel_type.lower() != "дизел":
            await update.message.reply_text("❌ Бу бўлим фақат дизел техника ҳайдовчилари ёки заправщик учун.")
            return

        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "diesel_give_status"
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        msg = await update.message.reply_text(
            "📌 ДИЗЕЛ бериш статуси\n\n"
            "Берилган дизел маълумотлари:\n"
            "Рўйхатдан маълумотни танланг.",
            reply_markup=diesel_give_status_keyboard(update.effective_user.id)
        )
        remember_inline_message(context, msg)
        return

    if text in ["⛽ ДИЗЕЛ расход", "⛽ ДИЗЕЛ бериш"]:
        work_role = get_driver_work_role(update.effective_user.id)

        if context.user_data.get("mode") not in ["diesel_menu", None]:
            await update.message.reply_text("❌ Бу амал фақат дизел менюсида ишлайди.")
            return

        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if work_role != "zapravshik" and fuel_type.lower() != "дизел":
            await update.message.reply_text("❌ ДИЗЕЛ расход фақат дизел техника ҳайдовчилари ёки заправщик учун.")
            return

        context.user_data["mode"] = "dieselgive_firm"
        context.user_data["dieselgive_from_car"] = driver_car if work_role != "zapravshik" else "Заправщик"
        if work_role == "zapravshik":
            context.user_data["dieselgive_from_name"] = get_driver_full_name_by_telegram_id(update.effective_user.id)

        await update.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=(
                diesel_prihod_firm_stock_keyboard()
                if is_zapravshik_diesel_expense_flow(context)
                else diesel_firm_plain_keyboard()
            )
        )
        return


    if text == "⬅️ Орқага" and mode == "diesel_menu":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        context.user_data.clear()

        await update.message.reply_text(
            driver_menu_text(update.effective_user.id),
            reply_markup=driver_main_keyboard(fuel_type)
        )
        return

    if text == "⬅️ Орқага" and mode in ["dieselgive_firm", "dieselgive_edit_firm"]:
        context.user_data["mode"] = "diesel_menu"

        await update.message.reply_text(
            "⛽ Дизел ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=diesel_report_keyboard(update.effective_user.id)
        )
        return

    if text == "⬅️ Орқага" and mode in ["dieselgive_car", "dieselgive_edit_car"]:
        if mode == "dieselgive_edit_car":
            context.user_data["mode"] = "dieselgive_edit_firm"
        else:
            context.user_data["mode"] = "dieselgive_firm"

        await update.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=(
                diesel_prihod_firm_stock_keyboard()
                if is_zapravshik_diesel_expense_flow(context)
                else diesel_firm_plain_keyboard()
            )
        )
        return

    if mode in ["dieselgive_firm", "dieselgive_edit_firm"]:
        firm_name = extract_firm_from_stock_button(text)

        reply_keyboard_for_firm = (
            diesel_prihod_firm_stock_keyboard()
            if is_zapravshik_diesel_expense_flow(context)
            else diesel_firm_plain_keyboard()
        )

        if text.startswith("📦 Бошқа дизел расходлар"):
            await update.message.reply_text(
                "❌ Бу менюдан оддий дизел расход қилиб бўлмайди.\n"
                "📦 Бошқа дизел расходлар алоҳида ҳисобланади.",
                reply_markup=reply_keyboard_for_firm
            )
            return

        if firm_name not in FIRM_NAMES:
            await update.message.reply_text(
                "❌ Фирмани пастки менюдан танланг.",
                reply_markup=reply_keyboard_for_firm
            )
            return

        if is_zapravshik_diesel_expense_flow(context):
            total_stock = get_total_company_diesel_stock()
            if total_stock <= 0:
                await update.message.reply_text(
                    "❌ Дизел остаткаси йўқ. Расход қилиб бўлмайди.\n\n"
                    f"Жами фирмалар остаткаси: {format_liter(total_stock)} л",
                    reply_markup=diesel_prihod_firm_stock_keyboard()
                )
                return

        context.user_data["dieselgive_firm"] = firm_name

        if mode == "dieselgive_edit_firm":
            context.user_data["mode"] = "dieselgive_edit_car"
        else:
            context.user_data["mode"] = "dieselgive_car"
    
        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
    
        await update.message.reply_text(
            "🚛 Қайси дизел техникага ДИЗЕЛ беряпсиз?",
            reply_markup=diesel_cars_by_firm_keyboard(
                firm_name,
                context.user_data.get("dieselgive_from_car")
            )
        )
        return

    if mode == "dieselgive_liter":

        if (
            update.message.photo
            or update.message.video
            or update.message.video_note
            or update.message.document
            or update.message.audio
            or update.message.voice
            or update.message.sticker
        ):
            await update.message.reply_text(
                "❌ Фақат литр миқдорини рақам билан киритинг.\n\n"
                "Мисол: 120",
                reply_markup=ReplyKeyboardRemove()
            )
            return
    
        if not is_valid_diesel_liter(text):
            await update.message.reply_text(
                "❌ Нотўғри литр миқдори.\n\n"
                "Фақат рақам киритинг.\n"
                "Мисол: 120",
                reply_markup=ReplyKeyboardRemove()
            )
            return
    
        if is_zapravshik_diesel_expense_flow(context):
            can_spend, total_stock, spend = can_spend_diesel_amount(text)
            if not can_spend:
                await update.message.reply_text(
                    "❌ Дизел остаткаси етарли эмас.\n\n"
                    f"Жами фирмалар остаткаси: {format_liter(total_stock)} л\n"
                    f"Сиз киритган расход: {format_liter(spend)} л\n\n"
                    "Бошқа дизел расходлар остаткаси бу ҳисобга қўшилмайди.",
                    reply_markup=back_keyboard()
                )
                return

        context.user_data["dieselgive_liter"] = text
        context.user_data["mode"] = "dieselgive_note"
    
        await update.message.reply_text(
            f"🚛 ДИЗЕЛ оладиган техника: {context.user_data.get('dieselgive_to_car')}\n"
            f"⛽ Литр: {text}\n\n"
            "🔴 Нега ДИЗЕЛ беряпсиз? Изоҳ ёзинг!\n\n"
            "Фақат текст киритинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "fuel_gas_edit_km":
        if not text.isdigit():
            await update.message.reply_text(
                "❌ Фақат рақам киритинг.\n\nМисол: 15300"
            )
            return

        context.user_data["fuel_km"] = text
        context.user_data["mode"] = "fuel_gas_confirm"

        await update.message.reply_text(
            fuel_gas_confirm_text(context),
            reply_markup=fuel_gas_after_action_keyboard()
        )
        return

    if mode == "fuel_gas_km":
        if not text.isdigit():
            await update.message.reply_text(
                "❌ Фақат рақам киритинг.\n\n"
                "🔴 Спидометр кўрсаткичини киритинг (КМ)\n"
                "Мисол: 15300"
            )
            return

        context.user_data["fuel_km"] = text
        context.user_data["mode"] = "fuel_gas_video"

        await update.message.reply_text(
            "🎥 Автомобил рақами ва ёқилғи қуйиш калонкаси якуний кўрсаткичини "
            "думалоқ видео қилиб ташланг.\n\n"
            "⏱ Видео 5 сониядан кам бўлмасин."
        )
        return

    if mode in ["fuel_gas_video", "fuel_gas_photo"]:
        await update.message.reply_text("❌ Бу босқичда матн қабул қилинмайди. Тўғри маълумот юборинг.")
        return

    if text == "🚚 Рўйхатдан ўтиш" and mode in [None, "driver_register_start"]:
        context.user_data["inline_disabled_by_start"] = False
        context.user_data["mode"] = "driver_name"

        await update.message.reply_text(
            "🔴 <b>Исмингизни киритинг</b>\n\nМисол: Тешавой",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "driver_name":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Исм фақат ҳарфлардан иборат бўлиши керак.\n\n"
                "🔴 <b>Мисол: Тешавой</b>",
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["driver_name"] = text
        context.user_data["mode"] = "driver_surname"
        
        await update.message.reply_text(
            "🔴 <b>Фамилиянгизни киритинг</b>\n\nМисол: Алиев",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "driver_surname":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Фамилия фақат ҳарфлардан иборат бўлиши керак.\n\n"
                "🔴 <b>Мисол: Алиев</b>",
                parse_mode="HTML",
            )
            return

    if mode == "driver_surname":
        context.user_data["driver_surname"] = text
        context.user_data["mode"] = "driver_phone"

        await update.message.reply_text(
            "📞 Телефон рақамингизни юборинг:",
            reply_markup=phone_keyboard()
        )
        return

    if mode in ["driver_phone", "driver_phone_edit"]:
        phone = clean_phone_number(text)

        if not is_valid_phone_number(phone):
            await update.message.reply_text(
                "❌ Телефон рақам нотўғри.\n\n"
                "Мисол: 998939992020",
                reply_markup=phone_keyboard()
            )
            return

        context.user_data["phone"] = phone

        if mode == "driver_phone_edit":
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        if context.user_data.get("driver_work_role") == "zapravshik":
            context.user_data["driver_firm"] = ""
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        context.user_data["mode"] = "driver_firm"

        await update.message.reply_text(
            "🏢 Қайси фирмада ишлайсиз?",
            reply_markup=firm_keyboard()
        )
        return    

        context.user_data["driver_surname"] = text
        context.user_data["mode"] = "driver_phone"
        
        await update.message.reply_text(
            "📞 Телефон рақамингизни юборинг:",
            reply_markup=phone_keyboard()
        )
        return

    if mode == "driver_firm":
        context.user_data["driver_firm"] = text

        if context.user_data.get("driver_work_role") == "mechanic":
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return

        context.user_data["mode"] = "driver_car"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        await update.message.reply_text(
            "🚛 Қайси техника ҳайдовчисисиз?",
            reply_markup=car_buttons_by_firm(text, only_available_for_driver=True)
        )
        return

    if mode == "driver_edit_firm_mechanic":
        context.user_data["driver_firm"] = text
        context.user_data["driver_car"] = ""
        context.user_data["mode"] = "driver_confirm"

        await show_driver_confirm(update.message, context)
        return

    if mode == "driver_edit_firm":
        context.user_data["driver_firm"] = text

        if context.user_data.get("driver_work_role") == "mechanic":
            context.user_data["driver_car"] = ""
            context.user_data["mode"] = "driver_confirm"
            await show_driver_confirm(update.message, context)
            return
        context.user_data["driver_car"] = ""
        context.user_data["mode"] = "driver_edit_car"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        await update.message.reply_text(
            f"🏢 Янги фирма: {text}\n\n🚛 Энди техникани қайта танланг:",
            reply_markup=car_buttons_by_firm(text, only_available_for_driver=True)
        )
        return

    if mode == "driver_edit_name":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Исм фақат ҳарфлардан иборат бўлиши керак.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["driver_name"] = text
        context.user_data["mode"] = "driver_confirm"

        await show_driver_confirm(update.message, context)
        return


    if mode == "driver_edit_surname":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Фамилия фақат ҳарфлардан иборат бўлиши керак.",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["driver_surname"] = text
        context.user_data["mode"] = "driver_confirm"

        await show_driver_confirm(update.message, context)
        return

    role = current_role

    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik"] and not str(context.user_data.get("mode", "")).startswith("driver"):
        driver_status = get_driver_status(update.effective_user.id)

        if driver_status == "Текширувда":
            await update.message.reply_text("⏳ Сизнинг аризангиз ҳали текширувда.")
            return

        if driver_status == "Рад этилди":
            await update.message.reply_text("❌ Сизнинг аризангиз рад этилган.")
            return

        if driver_status != "Тасдиқланди":
            await update.message.reply_text("Аввал рўйхатдан ўтинг.")
            return

    text = update.message.text.strip()
    mode = context.user_data.get("mode")
    current_role = current_role

    if mode == "driver_name":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Исм фақат ҳарфлардан иборат бўлиши керак.\n\n"
                "🔴 <b>Мисол: Тешавой</b>",
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
            return


    if mode == "driver_surname":
        if not is_valid_name(text):
            await update.message.reply_text(
                "❌ Фамилия фақат ҳарфлардан иборат бўлиши керак.\n\n"
                "🔴 <b>Мисол: Алиев</b>",
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
            return

        context.user_data["driver_surname"] = text
        context.user_data["mode"] = "driver_phone"

        await update.message.reply_text(
            "📞 Телефон рақамингизни юборинг:",
            reply_markup=phone_keyboard()
        )
        return

        context.user_data["driver_name"] = text
        context.user_data["mode"] = "driver_surname"

        await update.message.reply_text(
            "🔴 <b>Фамилиянгизни киритинг</b>\n\n"
            "Мисол: Алиев",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    if mode == "history_custom_period":
        try:
            start_text, end_text = text.split("-")

            start_date = datetime.strptime(start_text.strip(), "%d.%m.%Y")
            end_date = datetime.strptime(end_text.strip(), "%d.%m.%Y")
            end_date = end_date.replace(hour=23, minute=59, second=59)

            car = context.user_data.get("history_car")

            if not car:
                await update.message.reply_text("❌ Техника танланмаган.")
                return

            await send_history_by_date(update.message, car, start_date, end_date)

            context.user_data["mode"] = None
            return

        except Exception:
            await update.message.reply_text(
                "❌ Сана формати нотўғри.\n\n"
                "🔴 <b>Шундай ёзинг:</b>\n"
                "01.01.2026-28.04.2026",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

    if mode in ["send_km_photo", "edit_photo"]:
        await update.message.reply_text(
            "❌ Сиздан фақат одометр ёки моточас расмини юборишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return
    
    if mode in ["send_video", "edit_video"]:
        await update.message.reply_text(
            "❌ Сиздан фақат думалоқ видео хабар ёки видео файл юборишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return

    if mode == "gasgive_edit_firm":
        if text == "⬅️ Орқага":
            context.user_data["mode"] = "gasgive_edit_menu"
            await update.message.reply_text(
                "✏️ Қайси маълумотни таҳрирлайсиз?",
                reply_markup=ReplyKeyboardRemove()
            )
            await update.message.reply_text(
                "✏️ Таҳрирлаш менюси",
                reply_markup=gas_give_edit_keyboard()
            )
            return

        context.user_data["gasgive_firm"] = text
        context.user_data["mode"] = "gasgive_edit_car"

        await update.message.reply_text(
            "🚛 Қайси газли техникага ГАЗ беряпсиз?",
            reply_markup=gas_cars_by_firm_keyboard(
                text,
                context.user_data.get("gasgive_from_car"),
                callback_prefix="gasgive_edit_car"
            )
        )
        return

    if mode == "gasgive_firm":
        if text == "⬅️ Орқага":
            if mode == "fuel_menu":
                context.user_data.clear()

                driver_car = get_driver_car(update.effective_user.id)
                fuel_type = get_car_fuel_type(driver_car)

                await update.message.reply_text(
                    driver_menu_text(update.effective_user.id),
                    reply_markup=driver_main_keyboard(fuel_type)
                )
                return
            if mode == "gasgive_firm":
                context.user_data.clear()
                await update.message.reply_text(
                    "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
                    reply_markup=gas_report_keyboard()
                )
                return

            if mode == "gasgive_car":
                context.user_data["mode"] = "gasgive_firm"
                await update.message.reply_text(
                    "🏢 Қайси фирмадаги газли техникага ГАЗ беряпсиз?",
                    reply_markup=gas_firm_keyboard()
                )
                return

            if mode in ["gasgive_note", "gasgive_video", "gasgive_confirm"]:
                context.user_data["mode"] = "gasgive_car"
                firm = context.user_data.get("gasgive_firm")

                await update.message.reply_text(
                    "🚛 Қайси газли техникага ГАЗ беряпсиз?",
                    reply_markup=gas_cars_by_firm_keyboard(firm)
                )
                return
                
            context.user_data.clear()
            await update.message.reply_text(
                "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
                reply_markup=gas_report_keyboard()
            )
            return

        context.user_data["gasgive_firm"] = text
        context.user_data["mode"] = "gasgive_car"

        await update.message.reply_text(
            "🚛 Қайси газли техникага ГАЗ беряпсиз?",
            reply_markup=gas_cars_by_firm_keyboard(
                text,
                context.user_data.get("gasgive_from_car")
            )
        )
        return

    if mode == "gasgive_note":
        if not is_valid_gas_note(text):
            await update.message.reply_text(
                "❌ Изоҳ фақат ҳарф ва рақамдан иборат бўлиши керак.\n\n"
                "🔴 Нега ГАЗ беряпсиз? Изоҳ ёзинг."
            )
            return

        context.user_data["gasgive_note"] = text
        context.user_data["mode"] = "gasgive_video"

        await update.message.reply_text(
            "🎥 Бошқарувингиздаги техника ва ГАЗ оладиган техника номерлари билан "
            "газ бериш жараёнини видео қилиб ташланг.\n\n"
            "⏱ Видео 10 сониядан кам бўлмасин.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "gasgive_video":
        await update.message.reply_text(
            "❌ Бу босқичда фақат видео қабул қилинади.\n\n"
            "🎥 10 сониядан кам бўлмаган видео юборинг."
        )
        return

    if mode == "dieselgive_edit_liter":
        if not is_valid_diesel_liter(text):
            await update.message.reply_text(
                "❌ Нотўғри литр миқдори.\n\n"
                "⛽ Фақат рақам киритинг.\n"
                "Мисол: 60"
            )
            return

        context.user_data["dieselgive_liter"] = text
        context.user_data["mode"] = "dieselgive_confirm"

        await update.message.reply_text(
            diesel_confirm_text(context),
            reply_markup=diesel_give_final_keyboard()
        )
        return

    if mode == "dieselgive_edit_note":
        if not is_valid_gas_note(text):
            await update.message.reply_text(
                "❌ Изоҳ нотўғри.\n\n"
                "📝 Фақат ҳарф ва рақамдан фойдаланинг."
            )
            return

        context.user_data["dieselgive_note"] = text
        context.user_data["mode"] = "dieselgive_confirm"

        await update.message.reply_text(
            diesel_confirm_text(context),
            reply_markup=diesel_give_final_keyboard()
        )
        return

    if mode == "dieselgive_liter":
        await update.message.reply_text(
            "❌ Фақат литр миқдорини рақам билан киритинг.\n\n"
            "Мисол: 120",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    
    if mode == "dieselgive_note":
        await update.message.reply_text(
            "❌ Бу босқичда фақат текст қабул қилинади.\n\n"
            "📝 Изоҳни қайта киритинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return


    if text == "⬅️ Орқага":
        if mode == "technadzor_staff_edit_menu":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            driver_id = context.user_data.get("technadzor_selected_staff_id")
            context.user_data["mode"] = "technadzor_staff_card"
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        if mode in ["technadzor_staff_edit_name", "technadzor_staff_edit_surname", "technadzor_staff_edit_phone", "technadzor_staff_edit_role", "technadzor_staff_edit_firm", "technadzor_staff_edit_car"]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            driver_id = context.user_data.get("technadzor_selected_staff_id")

            if driver_id and technadzor_rollback_pending_edit(context, driver_id):
                context.user_data["mode"] = "technadzor_registration_list"
                await update.message.reply_text(
                    "↩️ Таҳрирлаш бекор қилинди. Эски маълумотлар тикланди.",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "Текширувда турган ходимлар:",
                    reply_markup=technadzor_pending_registration_keyboard()
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            context.user_data["mode"] = "technadzor_staff_card"
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        if mode == "technadzor_staff_card":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            staff_type = context.user_data.get("technadzor_staff_type", "drivers")
            firm = context.user_data.get("technadzor_staff_firm")
            context.user_data["mode"] = "technadzor_staff_drivers_list" if staff_type == "drivers" else f"technadzor_staff_{staff_type}_list"
            await update.message.reply_text(
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "Рўйхат:",
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        if mode == "technadzor_staff_menu":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data.clear()
            await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
            return

        if mode == "technadzor_staff_drivers_firm":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_menu"
            await update.message.reply_text("👥 Ходимлар менюси", reply_markup=technadzor_staff_menu_keyboard())
            return

        if mode == "technadzor_staff_drivers_list":
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_drivers_firm"
            context.user_data.pop("technadzor_staff_firm", None)
            await update.message.reply_text(
                "🚚 Ҳайдовчилар\n\nФирмани танланг:",
                reply_markup=technadzor_staff_firms_reply_keyboard()
            )
            return

        if mode in ["technadzor_staff_mechanics_list", "technadzor_staff_zapravshik_list"]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_menu"
            await update.message.reply_text("👥 Ходимлар менюси", reply_markup=technadzor_staff_menu_keyboard())
            return

        if mode == "driver_car":
            context.user_data["mode"] = "driver_firm"

            await update.message.reply_text(
                "🏢 Қайси фирмада ишлайсиз?",
                reply_markup=firm_keyboard()
            )
            return
            
        if mode == "choose_repair_type":
            if role == "technadzor":
                context.user_data["mode"] = "select_firm_for_add"
                await update.message.reply_text(
                    "🔴 <b>Фирмани танланг:</b>",
                    parse_mode="HTML",
                    reply_markup=firm_back_keyboard()
                )
                return


        if mode == "choose_car" or context.user_data.get("operation") == "add":
            push_state(context, "choose_repair_type")
            await update.message.reply_text("Ремонт турини танланг:", reply_markup=repair_type_keyboard())
            return

        if mode == "write_km":
            push_state(context, "choose_car")
            await update.message.reply_text("Техникани танланг:", reply_markup=car_buttons_by_firm(context.user_data.get("firm")))
            return

        if mode == "send_km_photo":
            push_state(context, "write_km")
            await update.message.reply_text(
                "🔴 <b>КМ/моточасни қайта киритинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if mode == "write_note_add":
            push_state(context, "send_km_photo")
            await update.message.reply_text(
                "🔴 <b>Одометр ёки моточас расмини қайта юборинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if mode == "write_note_remove":
            context.user_data["mode"] = "remove_car"
            await update.message.reply_text(
                "Носоз техникани танланг:",
                reply_markup=car_buttons_by_firm_and_status(context.user_data.get("firm"), "Носоз")
            )
            return

        if mode == "send_video":
            if context.user_data.get("operation") == "remove":
                context.user_data["mode"] = "write_note_remove"
            else:
                context.user_data["mode"] = "write_note_add"

            await update.message.reply_text(
                "🔴 <b>Изоҳни қайта ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if mode in ["edit_km", "edit_photo", "edit_note", "edit_video", "final_check"]:
            context.user_data["mode"] = "final_check"
            await update.message.reply_text(
                "🔴 <b>Маълумотни тасдиқлайсизми?</b>",
                parse_mode="HTML",
                reply_markup=final_confirm_keyboard()
            )
            return

        context.user_data.clear()

        if role == "technadzor":
            await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
            return

        if role == "mechanic":
            await update.message.reply_text("🔧 Механик менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
            return

        if role == "slesar":
            await update.message.reply_text("🛠 Слесарь менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
            return

        # V16: Ҳайдовчи Орқага/Отмен/режимдан чиққанда директор менюсига тушмасин.
        if role == "driver":
            driver_car = get_driver_car(update.effective_user.id)
            fuel_type = get_car_fuel_type(driver_car)
            await update.message.reply_text(
                driver_menu_text(update.effective_user.id),
                reply_markup=driver_main_keyboard(fuel_type)
            )
            return

        await update.message.reply_text("👨‍💼 Директор менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
        return

    if mode == "reject_reason":
        car = context.user_data.get("reject_car")
        reason = text
        inspector = get_user_name(update)
        current_time = now_text()

        update_car_status(car, "Носоз")

        row_id = len(remont_ws.get_all_values())

        remont_ws.append_row([
            row_id,
            current_time,
            car,
            "",
            "Текширувдан ўтмади",
            "Носоз",
            reason,
            "",
            "",
            inspector,
            "",
            current_time,
            "",
            update.effective_user.id
        ])

        await update.message.reply_text(
            f"❌ {car} Носоз ҳолатига қайтарилди.",
            reply_markup=cars_for_check_by_firm_group()
        )

        context.user_data["mode"] = None
        context.user_data["reject_car"] = None
        return

    if mode == "edit_km":
        if not is_valid_km(text):
            await update.message.reply_text(
                "1❌ Нотўғри.\n\n🔴 <b>Фақат 1–8 хонали рақам киритинг.</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["km"] = text
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            f"Текширинг:\n\n"
            f"🚛 Техника: {context.user_data.get('car')}\n"
            f"🔧 Ремонт тури: {context.user_data.get('repair_type') or 'Ремонтдан чиқарилди'}\n"
            f"⏱ КМ/Моточас: {context.user_data.get('km')}\n"
            f"📝 Изоҳ: {context.user_data.get('note')}\n"
            f"🎥 Видео: сақланди ✅\n\n"
            f"Маълумот тўғрими?",
            reply_markup=final_confirm_keyboard()
        )
        return

    if mode == "edit_note":
        if not is_valid_note(text):
            await update.message.reply_text(
                "❌ Изоҳ жуда қисқа.\n\n🔴 <b>Изоҳни ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["note"] = text
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            f"Текширинг:\n\n"
            f"🚛 Техника: {context.user_data.get('car')}\n"
            f"🔧 Ремонт тури: {context.user_data.get('repair_type') or 'Ремонтдан чиқарилди'}\n"
            f"⏱ КМ/Моточас: {context.user_data.get('km')}\n"
            f"📝 Изоҳ: {context.user_data.get('note')}\n"
            f"🎥 Видео: сақланди ✅\n\n"
            f"Маълумот тўғрими?",
            reply_markup=final_confirm_keyboard()
        )
        return

    if mode in ["technadzor_staff_edit_name", "technadzor_staff_edit_surname"]:
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        if not driver_id:
            await update.message.reply_text("❌ Ходим танланмаган.", reply_markup=technadzor_staff_menu_keyboard())
            return

        clean_text = text.strip()
        field_title = "Исм" if mode == "technadzor_staff_edit_name" else "Фамилия"

        if not is_valid_name(clean_text):
            await update.message.reply_text(
                f"❌ {field_title} фақат ҳарфлардан иборат бўлиши керак.\n\n"
                "Рақам, символ, расм ёки видео қабул қилинмайди.\n"
                "Мисол: Али",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            return

        column = "name" if mode == "technadzor_staff_edit_name" else "surname"
        try:
            cursor.execute(f"UPDATE drivers SET {column} = %s WHERE id = %s", (clean_text, int(driver_id)))
            conn.commit()
            sync_driver_to_google_sheet(driver_id=driver_id)
        except Exception as e:
            print("TECHNADZOR STAFF NAME UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        context.user_data["mode"] = "technadzor_staff_card"
        await clear_technadzor_staff_inline(context, update.effective_chat.id)
        await update.message.reply_text("✅ Маълумот сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
        msg = await update.message.reply_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return


    if mode == "technadzor_staff_edit_phone":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        phone = clean_phone_number(text)

        if not is_valid_phone_number(phone):
            await update.message.reply_text(
                "❌ Телефон рақам нотўғри.\n\n"
                "Фақат 998 билан бошланган 12 та рақам киритинг.\n"
                "Мисол: 998939992020",
                reply_markup=phone_back_keyboard()
            )
            return

        try:
            cursor.execute("UPDATE drivers SET phone = %s WHERE id = %s", (phone, int(driver_id)))
            conn.commit()
            sync_driver_to_google_sheet(driver_id=driver_id)
        except Exception as e:
            print("TECHNADZOR STAFF PHONE UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        context.user_data["mode"] = "technadzor_staff_card"
        await clear_technadzor_staff_inline(context, update.effective_chat.id)
        await update.message.reply_text("✅ Телефон сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
        msg = await update.message.reply_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return

    if mode == "technadzor_staff_edit_role":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        role_map = {
            "🚚 Ҳайдовчи": "driver",
            "🔧 Механик": "mechanic",
            "⛽ Заправщик": "zapravshik",
        }

        if text not in role_map:
            await update.message.reply_text("❌ Лавозимни пастки менюдан танланг.", reply_markup=technadzor_staff_role_reply_keyboard())
            return

        new_role = role_map[text]
        context.user_data["technadzor_staff_pending_role"] = new_role

        if not technadzor_normalize_staff_fields_for_role(driver_id, new_role):
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        # Заправщик регистрациясида фирма/техника йўқ — карточкага қайтади.
        if new_role == "zapravshik":
            context.user_data.pop("technadzor_staff_pending_role", None)
            context.user_data["mode"] = "technadzor_staff_card"
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        # Ҳайдовчи ва механик регистрациясида фирма танланади.
        # Ҳайдовчида фирмадан кейин техника ҳам танланади.
        context.user_data["mode"] = "technadzor_staff_edit_firm"
        await update.message.reply_text(
            "✅ Лавозим сақланди. Энди фирмани танланг:",
            reply_markup=technadzor_staff_firms_reply_keyboard()
        )
        return

    if mode == "technadzor_staff_edit_firm":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        if text not in FIRM_NAMES:
            await update.message.reply_text("❌ Фирмани пастки менюдан танланг.", reply_markup=technadzor_staff_firms_reply_keyboard())
            return

        staff = get_staff_by_id(driver_id)
        work_role = context.user_data.get("technadzor_staff_pending_role") or (staff or {}).get("work_role", "driver")

        if work_role == "zapravshik":
            await update.message.reply_text("❌ Заправщик учун фирма танланмайди.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        try:
            if work_role == "driver":
                cursor.execute(
                    "UPDATE drivers SET work_role = %s, firm = %s, car = NULL WHERE id = %s",
                    ("driver", text, int(driver_id))
                )
            else:
                cursor.execute(
                    "UPDATE drivers SET work_role = %s, firm = %s, car = NULL WHERE id = %s",
                    ("mechanic", text, int(driver_id))
                )
            conn.commit()
            sync_driver_to_google_sheet(driver_id=driver_id)
        except Exception as e:
            print("TECHNADZOR STAFF FIRM UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        if work_role == "driver":
            context.user_data["mode"] = "technadzor_staff_edit_car"
            await update.message.reply_text(
                "✅ Фирма ўзгарди. Энди шу фирма техникани танланг:",
                reply_markup=technadzor_staff_cars_reply_keyboard(text, exclude_driver_id=driver_id)
            )
            return

        context.user_data.pop("technadzor_staff_pending_role", None)
        context.user_data["mode"] = "technadzor_staff_card"
        await clear_technadzor_staff_inline(context, update.effective_chat.id)
        await update.message.reply_text("✅ Фирма сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
        msg = await update.message.reply_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return

    if mode == "technadzor_staff_edit_car":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        staff = get_staff_by_id(driver_id)
        firm = staff["firm"] if staff else ""

        try:
            cursor.execute("""
                SELECT car_number
                FROM cars
                WHERE firm = %s AND LOWER(car_number) = LOWER(%s)
                LIMIT 1
            """, (firm, text))
            row = cursor.fetchone()
        except Exception as e:
            print("TECHNADZOR STAFF CAR CHECK ERROR:", e)
            row = None

        if not row:
            await update.message.reply_text("❌ Техникани пастки менюдан танланг.", reply_markup=technadzor_staff_cars_reply_keyboard(firm, exclude_driver_id=driver_id))
            return

        car_number = row[0]
        try:
            cursor.execute("UPDATE drivers SET work_role = %s, car = %s WHERE id = %s", ("driver", car_number, int(driver_id)))
            conn.commit()
            sync_driver_to_google_sheet(driver_id=driver_id)
        except Exception as e:
            print("TECHNADZOR STAFF CAR UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        context.user_data.pop("technadzor_staff_pending_role", None)
        context.user_data["mode"] = "technadzor_staff_card"
        await clear_technadzor_staff_inline(context, update.effective_chat.id)
        await update.message.reply_text("✅ Техника сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
        msg = await update.message.reply_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return

    if role == "technadzor" and text == "⬅️ Орқага" and mode == "technadzor_registration_after_decision":
        await clear_technadzor_staff_inline(context, update.effective_chat.id)

        context.user_data.pop("operation", None)
        context.user_data.pop("firm", None)
        context.user_data.pop("car", None)
        context.user_data.pop("history_car", None)
        context.user_data.pop("technadzor_selected_staff_id", None)
        context.user_data.pop("technadzor_staff_action_stack", None)

        pending_count = pending_registration_count()

        if pending_count > 0:
            context.user_data["mode"] = "technadzor_registration_list"
            await update.message.reply_text(
                "📝 Регистрация текшируви",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "Текширувда турган ходимлар:",
                reply_markup=technadzor_pending_registration_keyboard()
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        context.user_data["mode"] = "technadzor_staff_menu"
        await update.message.reply_text(
            "👥 Ходимлар менюси",
            reply_markup=technadzor_staff_menu_keyboard()
        )
        return

    if role == "technadzor" and text == "⬅️ Орқага" and mode in [
        "technadzor_staff_edit_name",
        "technadzor_staff_edit_surname",
        "technadzor_staff_edit_phone",
        "technadzor_staff_edit_role",
        "technadzor_staff_edit_firm",
        "technadzor_staff_edit_car",
        "technadzor_staff_card",
    ]:
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        staff = get_staff_by_id(driver_id) if driver_id else None

        if driver_id:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)

            # Оддий ходим карточкаси Ходимлар рўйхатидан очилган бўлса,
            # пастки Орқага регистрацияга эмас, айнан ўша рўйхатга қайтаради.
            if staff and staff.get("status") != "Текширувда":
                stack = context.user_data.get("technadzor_staff_action_stack", [])
                last_list = None
                while stack:
                    item = stack.pop()
                    if item.get("screen") == "list":
                        last_list = item
                        break
                context.user_data["technadzor_staff_action_stack"] = stack

                staff_type = (last_list or {}).get("staff_type") or context.user_data.get("technadzor_staff_type") or get_staff_type_from_work_role(staff.get("work_role"))
                firm = (last_list or {}).get("firm") if last_list is not None else context.user_data.get("technadzor_staff_firm")

                context.user_data["technadzor_staff_type"] = staff_type
                context.user_data["technadzor_staff_firm"] = firm
                context.user_data["mode"] = "technadzor_staff_drivers_list" if staff_type == "drivers" else f"technadzor_staff_{staff_type}_list"

                await update.message.reply_text(
                    technadzor_staff_list_text(staff_type, firm),
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "Керакли ходимни танланг:",
                    reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            # Агар тасдиқлаш/рад этиш босилмаган бўлса — охирги таҳрирлашдан олдинги маълумотга қайтарилади.
            if not technadzor_is_pending_decision_done(context, driver_id):
                rolled_back = technadzor_rollback_pending_edit(context, driver_id)
                context.user_data["mode"] = "technadzor_registration_list"
                await update.message.reply_text(
                    "↩️ Таҳрирлаш бекор қилинди. Эски маълумотлар тикланди." if rolled_back else "↩️ Орқага қайтилди.",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "Текширувда турган ходимлар:",
                    reply_markup=technadzor_pending_registration_keyboard()
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            # Агар тасдиқлаш/рад этиш босилган бўлса:
            # текширувда яна ходим бўлса — ходим танлаш менюсига қайтади
            # ходим қолмаган бўлса — Ходимлар менюсига қайтади.

            pending_count = pending_registration_count()

            if pending_count > 0:
                context.user_data["mode"] = "technadzor_registration_list"

                await update.message.reply_text(
                    "📝 Регистрация текшируви",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )

                msg = await update.message.reply_text(
                    "Текширувда турган ходимлар:",
                    reply_markup=technadzor_pending_registration_keyboard()
                )

                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            context.user_data["mode"] = "technadzor_staff_menu"

            await update.message.reply_text(
                "👥 Ходимлар менюси",
                reply_markup=technadzor_staff_menu_keyboard()
            )
            return

    if role == "technadzor" and text == "⬅️ Орқага" and mode == "confirm_exit_card":
        await clear_all_inline_messages(context, update.effective_chat.id)
        context.user_data["mode"] = "confirm_exit"
        await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=only_back_keyboard())
        msg = await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
        remember_inline_message(context, msg)
        return

    if role == "technadzor":
        if text == "⬅️ Орқага" and mode in ["select_firm_for_add", "technadzor_staff_menu"]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data.clear()
            await update.message.reply_text(
                "👮 Текширувчи менюси",
                reply_markup=technadzor_keyboard()
            )
            return

        if text == "⬅️ Орқага" and mode in [
            "technadzor_registration_list",
            "technadzor_staff_drivers_firm",
            "technadzor_staff_drivers_list",
            "technadzor_staff_mechanics_list",
            "technadzor_staff_zapravshik_list",
        ]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_menu"
            await update.message.reply_text(
                "👥 Ходимлар менюси",
                reply_markup=technadzor_staff_menu_keyboard()
            )
            return

        if text.startswith("👥 Ходимлар"):
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_menu"
            await update.message.reply_text(
                "👥 Ходимлар менюси",
                reply_markup=technadzor_staff_menu_keyboard()
            )
            return

        if mode == "technadzor_staff_menu":
            if text == "🚚 Ҳайдовчилар":
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_staff_drivers_firm"
                await update.message.reply_text(
                    "🚚 Ҳайдовчилар\n\nФирмани танланг:",
                    reply_markup=technadzor_driver_firms_reply_keyboard()
                )
                return

            if text == "🔧 Механиклар":
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_staff_mechanics_list"
                context.user_data["technadzor_staff_type"] = "mechanics"
                context.user_data.pop("technadzor_staff_firm", None)
                context.user_data["technadzor_staff_action_stack"] = []
                await update.message.reply_text(
                    "🔧 Механиклар рўйхати",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "🔧 Механиклар:",
                    reply_markup=technadzor_staff_list_inline_keyboard("mechanics")
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            if text == "⛽ Заправщиклар":
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_staff_zapravshik_list"
                context.user_data["technadzor_staff_type"] = "zapravshik"
                context.user_data.pop("technadzor_staff_firm", None)
                context.user_data["technadzor_staff_action_stack"] = []
                await update.message.reply_text(
                    "⛽ Заправщиклар рўйхати",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "⛽ Заправщиклар:",
                    reply_markup=technadzor_staff_list_inline_keyboard("zapravshik")
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

            if text.startswith("📝 Регистрация"):
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_registration_list"
                context.user_data.pop("technadzor_registration_after_decision", None)
                context.user_data["technadzor_staff_action_stack"] = []
                await update.message.reply_text(
                    "📝 Регистрация текшируви",
                    reply_markup=technadzor_staff_back_reply_keyboard()
                )
                msg = await update.message.reply_text(
                    "Текширувда турган ходимлар:",
                    reply_markup=technadzor_pending_registration_keyboard()
                )
                context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
                remember_inline_message(context, msg)
                return

        if mode == "technadzor_staff_drivers_firm" and extract_firm_from_count_button(text) in FIRM_NAMES:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_drivers_list"
            context.user_data["technadzor_staff_type"] = "drivers"
            firm_name = extract_firm_from_count_button(text)
            context.user_data["technadzor_staff_firm"] = firm_name
            context.user_data["technadzor_staff_action_stack"] = []
            await update.message.reply_text(
                f"🚚 Ҳайдовчилар\n🏢 {firm_name}",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "🚚 Ҳайдовчилар рўйхати:",
                reply_markup=technadzor_staff_list_inline_keyboard("drivers", firm_name)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            remember_inline_message(context, msg)
            return

        if text == "🔧 Ремонтга қўшиш":
            context.user_data.clear()
            context.user_data["mode"] = "select_firm_for_add"
            await update.message.reply_text("🔴 <b>Фирмани танланг:</b>", parse_mode="HTML", reply_markup=firm_back_keyboard())
            return

        if text == "☑️ Ремонтдан чиқишини тасдиқлаш":
            context.user_data["mode"] = "confirm_exit"
            msg = await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
            remember_inline_message(context, msg)
            return

        if text == "📚 История":
            context.user_data["mode"] = "history_select_firm"
            await update.message.reply_text("Қайси фирма техникасини кўрмоқчисиз?", reply_markup=firm_keyboard())
            return

    if mode == "history_select_firm" and text in FIRM_NAMES:
        context.user_data["firm"] = text
        context.user_data["mode"] = "history_select_car"

        await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=back_keyboard())
        msg = await update.message.reply_text("Техникани танланг:", reply_markup=history_car_buttons_by_firm(text))
        remember_inline_message(context, msg)
        return

    if text in FIRM_NAMES:
        if mode == "select_firm_for_add":
            context.user_data["firm"] = text
            context.user_data["mode"] = "choose_repair_type"
            context.user_data["operation"] = "add"

            await update.message.reply_text("Ремонт турини танланг:", reply_markup=repair_type_keyboard())
            return

        context.user_data.clear()
        context.user_data["firm"] = text

        await update.message.reply_text(
            f"🏢 {text} танланди.\n\nАмални танланг:",
            reply_markup=action_keyboard()
        )
        return

    if text == "🔧 Ремонтга қўшиш":
        context.user_data["inline_disabled_by_start"] = False
        if not context.user_data.get("firm"):
            await update.message.reply_text("Аввал фирмани танланг.")
            return

        context.user_data["mode"] = "choose_repair_type"
        context.user_data["operation"] = "add"
        context.user_data["repair_type"] = None
        context.user_data["km"] = None
        context.user_data["km_photo_id"] = None
        context.user_data["video_id"] = None

        await update.message.reply_text("Ремонт турини танланг:", reply_markup=repair_type_keyboard())
        return

    if text == "✅ Ремонтдан чиқариш":
        context.user_data["inline_disabled_by_start"] = False
        if role not in ["director", "mechanic", "slesar"]:
            await deny(update)
            return

        if not context.user_data.get("firm"):
            await update.message.reply_text("Аввал фирмани танланг.")
            return

        context.user_data["mode"] = "remove_car"
        context.user_data["operation"] = "remove"
        context.user_data["repair_type"] = None
        context.user_data["km"] = ""
        context.user_data["km_photo_id"] = ""

        await update.message.reply_text(
            "Носоз техникани танланг:",
            reply_markup=car_buttons_by_firm_and_status(context.user_data["firm"], "Носоз")
        )
        return

    if text in REPAIR_TYPES:
        context.user_data["inline_disabled_by_start"] = False
        if not context.user_data.get("firm"):
            await update.message.reply_text("Аввал фирмани танланг.")
            return

        context.user_data["repair_type"] = text
        context.user_data["mode"] = "choose_car"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )
        await update.message.reply_text("Техникани танланг:", reply_markup=car_buttons_by_firm(context.user_data["firm"]))
        return

    if mode == "write_km":
        if not is_valid_km(text):
            await update.message.reply_text(
                "❌ Нотўғри.\n\n🔴 <b>Фақат 1–8 хонали рақам киритинг.</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["km"] = text
        context.user_data["mode"] = "send_km_photo"

        await update.message.reply_text(
            "✅ КМ/моточас сақланди.\n\n🔴 <b>Энди одометр ёки моточас расмини юборинг.</b>",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["write_note_add", "write_note_remove"]:
        if not is_valid_note(text):
            await update.message.reply_text(
                "❌ Изоҳ жуда қисқа.\n\n🔴 <b>Изоҳни ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["note"] = text
        context.user_data["mode"] = "send_video"

        await update.message.reply_text(
            "✅ Изоҳ сақланди.\n\n🔴 <b>Энди думалоқ видео ёки оддий видео файл юборинг.</b>",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    if mode == "send_km_photo":
        await update.message.reply_text(
            "❌ Бу босқичда фақат расм қабул қилинади.\n\n🔴 <b>Одометр ёки моточас расмини юборинг.</b>",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    if mode == "send_video":
        await update.message.reply_text(
            "❌ Бу босқичда фақат расм қабул қилинади.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    await update.message.reply_text("Менюдан тугмани танланг.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    if await v57_check_inactivity(update, context):
        return

    query = update.callback_query
    # === V70: BLOCK қилинган ходим inline callback ҳам ишлата олмайди ===
    if get_user(update) is None:
        current_status = get_driver_status(update.effective_user.id)
        if current_status == "Рад этилди":
            context.user_data.clear()
            try:
                await query.answer("⛔ Сиз блоклангансиз.", show_alert=True)
            except Exception:
                pass
            try:
                await query.message.reply_text(
                    "⛔ Сиз блоклангансиз. Администратор PLAY қилмагунча ботдан фойдалана олмайсиз.",
                    reply_markup=ReplyKeyboardRemove()
                )
            except Exception:
                pass
            return

    await query.answer()
    data = query.data

    if data == "none":
        return

    # === V50: Текширувчи → Отчет Дизел → История Дизел callbacks ===
    if data.startswith("tz_diesel_hist_back|"):
        target = data.split("|", 1)[1]
        await clear_all_inline_messages(context, update.effective_chat.id)

        if target == "period":
            context.user_data["mode"] = "technadzor_diesel_history_zap_period"
            try:
                await query.message.delete()
            except Exception:
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass
            await query.message.chat.send_message(
                "⛽ Заправщик дизел тарихи\n\nДаврни танланг:",
                reply_markup=technadzor_diesel_history_period_keyboard()
            )
            return

        if target == "driver_period":
            context.user_data["mode"] = "technadzor_diesel_history_driver_period"
            try:
                await query.message.delete()
            except Exception:
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass
            await query.message.chat.send_message(
                "🚛 Ҳайдовчи дизел тарихи\n\nДаврни танланг:",
                reply_markup=technadzor_diesel_history_period_keyboard()
            )
            return

        if target == "list":
            start_date = context.user_data.get("technadzor_diesel_history_start")
            end_date = context.user_data.get("technadzor_diesel_history_end")
            if not start_date or not end_date:
                context.user_data["mode"] = "technadzor_diesel_history_zap_period"
                await query.message.chat.send_message(
                    "⛽ Заправщик дизел тарихи\n\nДаврни қайта танланг:",
                    reply_markup=technadzor_diesel_history_period_keyboard()
                )
                return
            hist_target = context.user_data.get("technadzor_diesel_history_target")
            if hist_target == "driver":
                context.user_data["mode"] = "technadzor_diesel_history_driver_list"
                title = (
                    "🚛 Ҳайдовчи Дизел тарихи\n"
                    f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                    "Рўйхат тартиби:\nТехника номери / Фирма / Литр"
                )
                list_markup = technadzor_driver_diesel_history_list_keyboard(start_date, end_date)
            else:
                context.user_data["mode"] = "technadzor_diesel_history_zap_list"
                title = (
                    "⛽ Заправщик Дизел тарихи\n"
                    f"📅 Давр: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
                    "Рўйхат тартиби:\nТехника номери / Фирма / Литр"
                )
                list_markup = technadzor_zapravshik_diesel_history_list_keyboard(start_date, end_date)
            try:
                await query.edit_message_text(title, reply_markup=list_markup)
                remember_inline_message(context, query.message)
            except Exception:
                msg = await query.message.chat.send_message(title, reply_markup=list_markup)
                remember_inline_message(context, msg)
            return

    if data.startswith("tz_diesel_hist_card|"):
        try:
            _, kind, record_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        text_card, has_media = get_technadzor_diesel_history_card(kind, record_id, context)
        context.user_data["mode"] = "technadzor_diesel_history_card"
        try:
            await query.edit_message_text(
                text_card,
                reply_markup=technadzor_diesel_history_view_keyboard(kind, record_id, has_media=has_media)
            )
            remember_inline_message(context, query.message)
        except Exception:
            msg = await query.message.reply_text(
                text_card,
                reply_markup=technadzor_diesel_history_view_keyboard(kind, record_id, has_media=has_media)
            )
            remember_inline_message(context, msg)

        try:
            await query.message.chat.send_message(
                "🏠 Бош менюга қайтиш учун пастдаги тугмани босинг.",
                reply_markup=technadzor_main_menu_only_keyboard()
            )
        except Exception:
            pass
        return

    if data.startswith("tz_diesel_hist_view|"):
        try:
            _, kind, record_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        await send_technadzor_diesel_history_media(query, kind, record_id, context)
        return

    if data.startswith("driver_diesel_report_back|"):
        parts = data.split("|")
        back_target = parts[1] if len(parts) > 1 else "menu"

        if back_target == "menu":
            context.user_data["mode"] = "driver_reports_menu"
            try:
                await query.edit_message_text(
                    "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:"
                )
            except Exception:
                pass
            try:
                await query.message.reply_text(
                    "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:",
                    reply_markup=driver_reports_keyboard(update.effective_user.id)
                )
            except Exception:
                pass
            return

        if back_target == "list" and len(parts) >= 3:
            report_type = parts[2]
            if report_type == "received":
                context.user_data["mode"] = "driver_reports_received"
                received_count, _ = get_driver_diesel_report_counts(update.effective_user.id)
                title = f"📥 Приход Дизел маълумотлари ({received_count} та)\n\nСтатуси тасдиқланган дизел приход карточкалари:"
            else:
                context.user_data["mode"] = "driver_reports_given"
                _, given_count = get_driver_diesel_report_counts(update.effective_user.id)
                title = f"📤 Расход Дизел маълумотлари ({given_count} та)\n\nСтатуси тасдиқланган дизел расход карточкалари:"

            try:
                await query.edit_message_text(
                    title,
                    reply_markup=driver_diesel_report_list_keyboard(update.effective_user.id, report_type)
                )
            except Exception:
                msg = await query.message.reply_text(
                    title,
                    reply_markup=driver_diesel_report_list_keyboard(update.effective_user.id, report_type)
                )
                remember_inline_message(context, msg)
            return

    if data.startswith("driver_diesel_report_card|"):
        try:
            _, report_type, transfer_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        row = get_driver_diesel_report_row(transfer_id)
        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_value, video_id, created_at, status, approved_by_id = row
        user_id = int(update.effective_user.id)

        if status != "Тасдиқланди":
            await query.message.reply_text("❌ Бу маълумот тасдиқланмаган.")
            return

        if report_type == "received" and int(to_driver_id or 0) != user_id:
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        if report_type == "given" and int(from_driver_id or 0) != user_id:
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        try:
            await query.edit_message_text(
                driver_diesel_report_card_text(row, report_type),
                reply_markup=driver_diesel_report_view_keyboard(report_type, transfer_id, has_media=bool(video_id))
            )
            remember_inline_message(context, query.message)
        except Exception:
            msg = await query.message.reply_text(
                driver_diesel_report_card_text(row, report_type),
                reply_markup=driver_diesel_report_view_keyboard(report_type, transfer_id, has_media=bool(video_id))
            )
            remember_inline_message(context, msg)
        return

    if data.startswith("driver_diesel_report_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        try:
            _, report_type, transfer_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        row = get_driver_diesel_report_row(transfer_id)
        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_value, video_id, created_at, status, approved_by_id = row
        user_id = int(update.effective_user.id)

        if status != "Тасдиқланди":
            await query.message.reply_text("❌ Бу маълумот тасдиқланмаган.")
            return

        if report_type == "received" and int(to_driver_id or 0) != user_id:
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        if report_type == "given" and int(from_driver_id or 0) != user_id:
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        await query.message.reply_text(driver_diesel_report_card_text(row, report_type))

        if video_id:
            try:
                await context.bot.send_video_note(chat_id=query.message.chat_id, video_note=video_id)
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, video_id)
        return


    if data.startswith("driver_gas_report_back|"):
        parts = data.split("|")
        back_target = parts[1] if len(parts) > 1 else "menu"

        if back_target == "menu":
            context.user_data["mode"] = "driver_reports_menu"
            try:
                await query.edit_message_text(
                    "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:"
                )
            except Exception:
                pass
            try:
                await query.message.reply_text(
                    "📊 Ҳисоботлар менюси\n\nКеракли бўлимни танланг:",
                    reply_markup=driver_reports_keyboard(update.effective_user.id)
                )
            except Exception:
                pass
            return

    if data.startswith("driver_gas_report_card|"):
        try:
            _, report_type, record_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        user_id = int(update.effective_user.id)

        if report_type == "received":
            row = get_driver_gas_received_report_row(record_id)
            if not row:
                await query.message.reply_text("❌ Маълумот топилмади.")
                return
            _, telegram_id, car, fuel_type, km, video_id, photo_id, created_at = row
            if int(telegram_id or 0) != user_id:
                await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
                return
            card_text = driver_gas_received_card_text(row)
            has_media = bool(video_id or photo_id)
        else:
            row = get_driver_gas_given_report_row(record_id)
            if not row:
                await query.message.reply_text("❌ Маълумот топилмади.")
                return
            from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at, status, approved_by_id = row
            if status != "Тасдиқланди":
                await query.message.reply_text("❌ Бу маълумот тасдиқланмаган.")
                return
            if int(from_driver_id or 0) != user_id:
                await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
                return
            card_text = driver_gas_given_card_text(row)
            has_media = bool(video_id)

        try:
            await query.edit_message_text(
                card_text,
                reply_markup=driver_gas_report_view_keyboard(report_type, record_id, has_media=has_media)
            )
            remember_inline_message(context, query.message)
        except Exception:
            msg = await query.message.reply_text(
                card_text,
                reply_markup=driver_gas_report_view_keyboard(report_type, record_id, has_media=has_media)
            )
            remember_inline_message(context, msg)
        return

    if data.startswith("driver_gas_report_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        try:
            _, report_type, record_id = data.split("|", 2)
        except ValueError:
            await query.message.reply_text("❌ Нотўғри callback маълумоти.")
            return

        user_id = int(update.effective_user.id)

        if report_type == "received":
            row = get_driver_gas_received_report_row(record_id)
            if not row:
                await query.message.reply_text("❌ Маълумот топилмади.")
                return
            _, telegram_id, car, fuel_type, km, video_id, photo_id, created_at = row
            if int(telegram_id or 0) != user_id:
                await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
                return
            card_text = driver_gas_received_card_text(row)
            media_video_id = video_id
            media_photo_id = photo_id
        else:
            row = get_driver_gas_given_report_row(record_id)
            if not row:
                await query.message.reply_text("❌ Маълумот топилмади.")
                return
            from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at, status, approved_by_id = row
            if status != "Тасдиқланди":
                await query.message.reply_text("❌ Бу маълумот тасдиқланмаган.")
                return
            if int(from_driver_id or 0) != user_id:
                await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
                return
            card_text = driver_gas_given_card_text(row)
            media_video_id = video_id
            media_photo_id = None

        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        await query.message.reply_text(card_text)

        if media_photo_id:
            try:
                await context.bot.send_photo(chat_id=query.message.chat_id, photo=media_photo_id)
            except Exception:
                pass

        if media_video_id:
            try:
                await context.bot.send_video_note(chat_id=query.message.chat_id, video_note=media_video_id)
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, media_video_id)
        return

    # === V74: Заправшик уведомления рўйхатидан карточка очиш ===
    if data.startswith("znotif_prihod_return|"):
        record_id = data.split("|", 1)[1]
        await send_zapravshik_prihod_notification_card(query, context, record_id, returned=True)
        return

    if data.startswith("znotif_prihod_pending|"):
        record_id = data.split("|", 1)[1]
        await send_zapravshik_prihod_notification_card(query, context, record_id, returned=False)
        return

    if data.startswith("znotif_diesel_rejected|"):
        transfer_id = data.split("|", 1)[1]
        await send_zapravshik_diesel_notification_card(query, context, transfer_id, rejected=True)
        return

    if data.startswith("znotif_diesel_pending|"):
        transfer_id = data.split("|", 1)[1]
        await send_zapravshik_diesel_notification_card(query, context, transfer_id, rejected=False)
        return

    if data.startswith("znotif_prihod_pending_media|"):
        record_id = data.split("|", 1)[1]
        await send_zapravshik_prihod_pending_media(query, context, record_id)
        return

    if data.startswith("diesel_prihod_return_media|"):
        record_id = data.split("|", 1)[1]
        await send_zapravshik_prihod_returned_media(query, context, record_id)
        return

    if data.startswith("znotif_diesel_pending_media|"):
        transfer_id = data.split("|", 1)[1]
        row = get_diesel_transfer_full_row(transfer_id)
        if not row:
            await query.answer("Маълумот топилмади.", show_alert=True)
            return

        if int(row[1] or 0) != int(query.from_user.id):
            await query.answer("Бу маълумот сиз учун эмас.", show_alert=True)
            return

        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        await query.message.chat.send_message(
            diesel_transfer_sender_card_text(row, status_text="Қабул қилувчи текширувида")
        )

        video_id = row[8]
        if video_id:
            try:
                await query.message.chat.send_video_note(video_note=video_id)
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, video_id)
        return

    # === PRIORITY FIX: дизел приход медиа кўриш ===
    if data.startswith("diesel_prihod_media|"):
        context.user_data["inline_disabled_by_start"] = False
        record_id = data.split("|", 1)[1]
        await send_diesel_prihod_media_for_technadzor(query, context, record_id)
        return

    # === PRIORITY FIX: дизел приход рўйхатидан Кўриш ===
    if data.startswith("diesel_prihod_view|"):
        context.user_data["inline_disabled_by_start"] = False
        context.user_data["mode"] = "technadzor_diesel_prihod_card"

        record_id = data.split("|", 1)[1]

        await open_diesel_prihod_for_technadzor(query, context, record_id)
        return

    # === Заправщик/Текширувчи: дизел приход callbacks ===
    if data.startswith("diesel_prihod_tech_edit|") or data.startswith("diesel_prihod_return_edit|"):
        parts = data.split("|")

        if len(parts) == 2:
            action, record_id = parts

            if not diesel_prihod_row_to_context(record_id, context):
                await query.answer("Маълумот топилмади.", show_alert=True)
                return

            context.user_data["diesel_prihod_editing_db_id"] = record_id
            context.user_data["diesel_prihod_current_id"] = record_id
            context.user_data["diesel_prihod_edit_source"] = "returned" if action == "diesel_prihod_return_edit" else "tech"
            context.user_data["mode"] = "diesel_prihod_db_edit_menu"

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            edit_msg = await query.message.reply_text(
                "✏️ Қайси маълумотни таҳрирлайсиз?",
                reply_markup=diesel_prihod_edit_keyboard(f"{action}|{record_id}")
            )
            remember_inline_message(context, edit_msg)

            await query.message.reply_text(
                "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
                reply_markup=only_back_keyboard()
            )
            return

        if len(parts) == 3:
            action, record_id, field = parts
            if not diesel_prihod_row_to_context(record_id, context):
                await query.answer("Маълумот топилмади.", show_alert=True)
                return

            context.user_data["diesel_prihod_editing_db_id"] = record_id
            context.user_data["diesel_prihod_edit_source"] = "returned" if action == "diesel_prihod_return_edit" else "tech"

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            if field == "firm":
                context.user_data["mode"] = "diesel_prihod_db_edit_firm"
                await query.message.reply_text("🏢 Янги фирмани танланг:", reply_markup=diesel_prihod_firm_stock_keyboard())
                return
            if field == "firm":
                context.user_data["mode"] = "diesel_prihod_db_edit_firm"
                await query.message.reply_text("🏢 Янги фирмани танланг:", reply_markup=diesel_prihod_firm_stock_keyboard())
                return
            if field == "liter":
                context.user_data["mode"] = "diesel_prihod_db_edit_liter"
                await query.message.reply_text("⛽ Янги литрни киритинг. Фақат сон.", reply_markup=only_back_keyboard())
                return
            if field == "note":
                context.user_data["mode"] = "diesel_prihod_db_edit_note"
                await query.message.reply_text("📝 Янги изоҳни киритинг. Фақат текст ва рақам.", reply_markup=only_back_keyboard())
                return
            if field == "video":
                context.user_data["mode"] = "diesel_prihod_db_edit_video"
                await query.message.reply_text("🎥 Янги 10 секунддан кам бўлмаган думалоқ видеони юборинг.", reply_markup=only_back_keyboard())
                return
            if field == "photo":
                context.user_data["mode"] = "diesel_prihod_db_edit_photo"
                await query.message.reply_text("🖼 Янги накладной расмини юборинг.", reply_markup=only_back_keyboard())
                return

    if data == "other_diesel_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data.clear()
        await query.message.reply_text(
            zapravka_info_text(),
            reply_markup=zapravshik_main_keyboard()
        )
        return

    if data == "other_diesel_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "other_diesel_liter"
        await query.message.reply_text(
            "⛽ Янги литрни киритинг.",
            reply_markup=back_keyboard()
        )
        return


    if data == "other_diesel_view":
        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        # Янги карточка
        await query.message.chat.send_message(other_diesel_card_text(context, status="------"))

        # Видео
        video_id = context.user_data.get("other_diesel_video_id")
        if video_id:
            try:
                await query.message.chat.send_video_note(video_note=video_id)
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, video_id)

        # Энг пастда кнопкалар, Кўриш қайта чиқмайди
        await query.message.chat.send_message(
            "Маълумот тўғрими?",
            reply_markup=other_diesel_after_view_keyboard()
        )
        return


    if data == "other_diesel_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        can_spend, total_stock, spend = can_spend_diesel_amount(context.user_data.get("other_diesel_liter"))
        if not can_spend:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            await query.message.reply_text(
                "❌ Дизел остаткаси етарли эмас.\n\n"
                f"Жами фирмалар остаткаси: {format_liter(total_stock)} л\n"
                f"Сиз киритган расход: {format_liter(spend)} л",
                reply_markup=zapravshik_diesel_menu_keyboard()
            )
            return

        try:
            cursor.execute("""
                INSERT INTO diesel_other_expense
                (telegram_id, liter, note, video_id, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                query.from_user.id,
                context.user_data.get("other_diesel_liter"),
                context.user_data.get("other_diesel_note"),
                context.user_data.get("other_diesel_video_id"),
                "Тасдиқланди"
            ))
            conn.commit()
        except Exception as e:
            print("OTHER DIESEL INSERT ERROR:", e)
            await query.message.reply_text("❌ Базага сақлашда хато.")
            return

        context.user_data.clear()
        context.user_data["mode"] = "zapravshik_diesel_menu"

        await query.message.reply_text(
            "✅ Бошқа дизел расход сақланди.",
            reply_markup=zapravshik_main_keyboard()
        )
        await query.message.reply_text(
            zapravka_info_text(),
            reply_markup=zapravshik_main_keyboard()
        )
        return

    if data == "diesel_prihod_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        firm = context.user_data.get("diesel_prihod_firm")
        liter = context.user_data.get("diesel_prihod_liter")
        note = context.user_data.get("diesel_prihod_note")
        video_id = context.user_data.get("diesel_prihod_video_id")
        photo_id = context.user_data.get("diesel_prihod_photo_id")

        if not all([firm, liter, note, video_id, photo_id]):
            await query.message.reply_text("❌ Маълумот тўлиқ эмас. Қайта киритинг.", reply_markup=zapravshik_diesel_menu_keyboard())
            context.user_data["mode"] = "zapravshik_diesel_menu"
            return

        try:
            cursor.execute("""
                INSERT INTO diesel_prihod (telegram_id, liter, note, video_id, photo_id, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """, (
                update.effective_user.id,
                liter,
                get_diesel_prihod_note_for_db(context),
                video_id,
                photo_id,
                "Текширувда",
            ))
            record_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD INSERT ERROR:", e)
            await query.message.reply_text("❌ Базага сақлашда хато.", reply_markup=zapravshik_diesel_menu_keyboard())
            return

        context.user_data.clear()
        context.user_data["mode"] = "zapravshik_diesel_menu"

        await query.message.reply_text(
            "✅ Дизел приход текширувчига юборилди.\n📌 Статус: Текширувда",
            reply_markup=zapravshik_diesel_menu_keyboard()
        )
        await send_diesel_prihod_to_technadzor(context, record_id)
        return

    if data == "diesel_prihod_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data.clear()
        context.user_data["mode"] = "zapravshik_diesel_menu"
        await query.message.reply_text("❌ Дизел приход бекор қилинди.", reply_markup=zapravshik_diesel_menu_keyboard())
        return

    if data == "diesel_prihod_edit":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_menu"
        await query.message.reply_text("✏️ Қайси маълумотни таҳрирлайсиз?", reply_markup=diesel_prihod_edit_keyboard("diesel_prihod_edit"))
        return

    if data == "diesel_prihod_edit_firm":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_firm"
        await query.message.reply_text("🏢 Янги фирмани танланг:", reply_markup=diesel_prihod_firm_stock_keyboard())
        return

    if data == "diesel_prihod_edit_liter":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_liter"
        await query.message.reply_text("⛽ Янги литрни киритинг. Фақат сон.", reply_markup=only_back_keyboard())
        return

    if data == "diesel_prihod_edit_note":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_note"
        await query.message.reply_text("📝 Янги изоҳни киритинг. Фақат текст ва рақам.", reply_markup=only_back_keyboard())
        return

    if data == "diesel_prihod_edit_video":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_video"
        await query.message.reply_text("🎥 Янги 10 секунддан кам бўлмаган думалоқ видеони юборинг.", reply_markup=only_back_keyboard())
        return

    if data == "diesel_prihod_edit_photo":
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data["mode"] = "diesel_prihod_edit_photo"
        await query.message.reply_text("🖼 Янги накладной расмини юборинг.", reply_markup=only_back_keyboard())
        return

    if data.startswith("diesel_prihod_approve|"):
        record_id = data.split("|", 1)[1]
        try:
            await apply_diesel_prihod_staged_edits(context, record_id)

            approver_id = int(update.effective_user.id)
            approver_name = get_employee_full_name_by_telegram_id(approver_id)

            cursor.execute("""
                UPDATE diesel_prihod
                SET status = %s,
                    approved_by_id = %s,
                    approved_by_name = %s
                WHERE id = %s
            """, ("Тасдиқланди", approver_id, approver_name, int(record_id)))
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD APPROVE ERROR:", e)
            await query.answer("❌ Сақлашда хато.", show_alert=True)
            return

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text("✅ Дизел приход тасдиқланди.")
        row = diesel_prihod_row_to_context(record_id, context)
        if row:
            sender_id = context.user_data.get("diesel_prihod_telegram_id")
            try:
                await context.bot.send_message(int(sender_id), "✅ Дизел приход текширувчи томонидан тасдиқланди.")
            except Exception:
                pass
        return

    if data.startswith("diesel_prihod_reject|"):
        record_id = data.split("|", 1)[1]
        context.user_data["mode"] = "diesel_prihod_reject_note"
        context.user_data["diesel_prihod_reject_id"] = record_id
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("📝 Рад этиш изоҳини киритинг. Фақат текст ва рақам.", reply_markup=back_keyboard())
        return

    if data.startswith("diesel_prihod_tech_edit|"):
        record_id = data.split("|", 1)[1]
        if not diesel_prihod_row_to_context(record_id, context):
            await query.answer("Маълумот топилмади.", show_alert=True)
            return
        context.user_data["mode"] = "diesel_prihod_tech_edit_menu"
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("✏️ Қайси маълумотни таҳрирлайсиз?", reply_markup=diesel_prihod_edit_keyboard(f"diesel_prihod_tech_edit|{record_id}"))
        return

    if data.startswith("diesel_prihod_tech_edit|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, record_id, field = parts
            if not diesel_prihod_row_to_context(record_id, context):
                await query.answer("Маълумот топилмади.", show_alert=True)
                return
            context.user_data["diesel_prihod_editing_db_id"] = record_id
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            if field == "liter":
                context.user_data["mode"] = "diesel_prihod_db_edit_liter"
                await query.message.reply_text("⛽ Янги литрни киритинг. Фақат сон.", reply_markup=only_back_keyboard())
                return
            if field == "note":
                context.user_data["mode"] = "diesel_prihod_db_edit_note"
                await query.message.reply_text("📝 Янги изоҳни киритинг. Фақат текст ва рақам.", reply_markup=only_back_keyboard())
                return
            if field == "video":
                context.user_data["mode"] = "diesel_prihod_db_edit_video"
                await query.message.reply_text("🎥 Янги 10 секунддан кам бўлмаган думалоқ видеони юборинг.", reply_markup=only_back_keyboard())
                return
            if field == "photo":
                context.user_data["mode"] = "diesel_prihod_db_edit_photo"
                await query.message.reply_text("🖼 Янги накладной расмини юборинг.", reply_markup=only_back_keyboard())
                return

    if data.startswith("diesel_prihod_resend|"):
        record_id = data.split("|", 1)[1]
        try:
            await apply_diesel_prihod_staged_edits(context, record_id)

            cursor.execute("""
                UPDATE diesel_prihod
                SET status = %s,
                    note = regexp_replace(COALESCE(note, ''), E'\\nРад изоҳи: .*$', '')
                WHERE id = %s
            """, ("Текширувда", int(record_id)))
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD RESEND ERROR:", e)
            await query.answer("❌ Сақлашда хато.", show_alert=True)
            return
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("✅ Дизел приход қайта текширувга юборилди.", reply_markup=zapravshik_diesel_menu_keyboard())
        await send_diesel_prihod_to_technadzor(context, record_id)
        return

    if data.startswith("diesel_prihod_return_edit|"):
        record_id = data.split("|", 1)[1]
        if not diesel_prihod_row_to_context(record_id, context):
            await query.answer("Маълумот топилмади.", show_alert=True)
            return
        context.user_data["diesel_prihod_editing_db_id"] = record_id
        context.user_data["mode"] = "diesel_prihod_return_edit_menu"
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("✏️ Қайси маълумотни таҳрирлайсиз?", reply_markup=diesel_prihod_edit_keyboard(f"diesel_prihod_return_edit|{record_id}"))
        return

    if data.startswith("diesel_prihod_return_edit|"):
        parts = data.split("|")
        if len(parts) == 3:
            _, record_id, field = parts
            if not diesel_prihod_row_to_context(record_id, context):
                await query.answer("Маълумот топилмади.", show_alert=True)
                return
            context.user_data["diesel_prihod_editing_db_id"] = record_id
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            if field == "liter":
                context.user_data["mode"] = "diesel_prihod_db_edit_liter"
                await query.message.reply_text("⛽ Янги литрни киритинг. Фақат сон.", reply_markup=only_back_keyboard())
                return
            if field == "note":
                context.user_data["mode"] = "diesel_prihod_db_edit_note"
                await query.message.reply_text("📝 Янги изоҳни киритинг. Фақат текст ва рақам.", reply_markup=only_back_keyboard())
                return
            if field == "video":
                context.user_data["mode"] = "diesel_prihod_db_edit_video"
                await query.message.reply_text("🎥 Янги 10 секунддан кам бўлмаган думалоқ видеони юборинг.", reply_markup=only_back_keyboard())
                return
            if field == "photo":
                context.user_data["mode"] = "diesel_prihod_db_edit_photo"
                await query.message.reply_text("🖼 Янги накладной расмини юборинг.", reply_markup=only_back_keyboard())
                return

    if data.startswith("diesel_prihod_return_cancel|"):
        record_id = data.split("|", 1)[1]

        try:
            cursor.execute("DELETE FROM diesel_prihod WHERE id = %s", (int(record_id),))
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD CANCEL DELETE ERROR:", e)
        try:
            cursor.execute("DELETE FROM diesel_prihod WHERE id = %s", (int(record_id),))
            conn.commit()
        except Exception as e:
            print("DIESEL PRIHOD RETURN CANCEL ERROR:", e)
            await query.answer("❌ Ўчиришда хато.", show_alert=True)
            return
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("❌ Дизел приход отмен қилинди ва базадан ўчирилди.", reply_markup=zapravshik_diesel_menu_keyboard())
        return

    # === PRIORITY: Регистрация текшируви тасдиқлаш/рад этиш ===
    if data.startswith("tz_reg_approve|") or data.startswith("tz_reg_reject|"):
        driver_id = data.split("|", 1)[1]
        staff = get_staff_by_id(driver_id)

        if not staff:
            await query.answer("Ходим топилмади.", show_alert=True)
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        new_status = "Тасдиқланди" if data.startswith("tz_reg_approve|") else "Рад этилди"

        try:
            cursor.execute("UPDATE drivers SET status = %s WHERE id = %s", (new_status, int(driver_id)))
            conn.commit()
        except Exception as e:
            print("TZ REG STATUS UPDATE ERROR:", e)
            await query.answer("❌ Сақлашда хато.", show_alert=True)
            return

        technadzor_clear_pending_edit_backup(context, driver_id)
        technadzor_pending_decision_done(context, driver_id)
        sync_driver_to_google_sheet(driver_id=driver_id)
        await notify_registered_employee(context, staff.get("telegram_id"), new_status, staff.get("work_role", "driver"))

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Кейин пастки ⬅️ Орқага босилганда бошқа flow'га кириб кетмаслиги учун
        # махсус mode қўямиз ва ходимлар/ремонт state'ларини тозалаймиз.
        context.user_data["mode"] = "technadzor_registration_after_decision"
        context.user_data["technadzor_registration_after_decision"] = True
        context.user_data["technadzor_selected_staff_id"] = str(driver_id)
        context.user_data.pop("operation", None)
        context.user_data.pop("firm", None)
        context.user_data.pop("car", None)
        context.user_data.pop("history_car", None)
        context.user_data.pop("technadzor_staff_action_stack", None)

        await query.message.reply_text(
            "✅ Ходим тасдиқланди" if new_status == "Тасдиқланди" else "❌ Ходим рад этилди",
            reply_markup=technadzor_staff_back_reply_keyboard()
        )
        return

    # === PRIORITY: регистрация таҳрирлаш кнопкалари ===
    if data.startswith("driver_edit|"):
        context.user_data["inline_disabled_by_start"] = False

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        field = data.split("|", 1)[1]

        if field == "name":
            context.user_data["mode"] = "driver_edit_name"
            await query.message.reply_text(
                "🔴 <b>Янги исмни киритинг</b>\n\nМисол: Тешавой",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "surname":
            context.user_data["mode"] = "driver_edit_surname"
            await query.message.reply_text(
                "🔴 <b>Янги фамилияни киритинг</b>\n\nМисол: Алиев",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "phone":
            context.user_data["mode"] = "driver_phone_edit"
            await query.message.reply_text(
                "📞 Янги телефон рақамни юборинг:",
                reply_markup=phone_keyboard()
            )
            return

        if field == "role":
            context.user_data["mode"] = "driver_edit_role"
            await query.message.reply_text(
                "🪪 Янги лавозимни танланг:",
                reply_markup=register_role_keyboard()
            )
            return

        if field == "firm":
            if context.user_data.get("driver_work_role") == "zapravshik":
                await query.message.reply_text(
                    "❌ Заправщик учун фирма танлаш керак эмас.",
                    reply_markup=register_edit_keyboard(context)
                )
                return

            if context.user_data.get("driver_work_role") == "mechanic":
                context.user_data["mode"] = "driver_edit_firm_mechanic"
            else:
                context.user_data["mode"] = "driver_edit_firm"

            await query.message.reply_text(
                "🏢 Янги фирмани танланг:",
                reply_markup=firm_keyboard()
            )
            return

        if field == "car":
            if context.user_data.get("driver_work_role") != "driver":
                await query.message.reply_text(
                    "❌ Бу лавозим учун техника таҳрирлаш керак эмас.",
                    reply_markup=register_edit_keyboard(context)
                )
                return

            firm = context.user_data.get("driver_firm")
            context.user_data["mode"] = "driver_edit_car"
            await query.message.reply_text(
                "🚛 Янги техникани танланг:",
                reply_markup=car_buttons_by_firm(firm, only_available_for_driver=True)
            )
            return

    # === PRIORITY END ===

    # === PRIORITY FIX: регистрация ва ремонт inline кнопкалари ===
    # Бу блок /start ҳимояси ва пастдаги умумий role-check'лардан ОЛДИН ишлайди.
    # Шунинг учун регистрациядаги Тасдиқлаш/Таҳрирлаш ва ремонтдаги техника танлаш блокланмайди.
    if data in ["confirm_driver", "edit_driver"] or data.startswith("driver_edit|") or data.startswith("car_") or data.startswith("car|"):
        context.user_data["inline_disabled_by_start"] = False

    if data.startswith("car_") or data.startswith("car|"):
        car = data.split("|", 1)[1] if data.startswith("car|") else data.replace("car_", "", 1)
        mode = context.user_data.get("mode")
        operation = context.user_data.get("operation")

        # HISTORY
        if mode == "history_select_car":
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["history_car"] = car
            context.user_data["mode"] = "history_period"

            msg = await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
            remember_inline_message(context, msg)
            return

        # REPAIR ADD
        if mode == "choose_car" or operation == "add" or mode in ["repair_add_car", "mechanic_repair_add_car"]:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            context.user_data["operation"] = "add"

            repair_type = context.user_data.get("repair_type")

            await send_last_repairs(query, car, repair_type)

            context.user_data["mode"] = "write_km"

            await query.message.reply_text(
                f"🚛 Техника: {car}\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\n"
                f"🔧 Ремонт тури: {repair_type}\n\n"
                "🔴 <b>Юрган масофа ёки моточасни киритинг:</b>\n\n"
                "Мисол: 125000",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        # REPAIR REMOVE
        if mode == "remove_car" or operation == "remove" or mode in ["repair_exit_car", "mechanic_repair_exit_car"]:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            context.user_data["operation"] = "remove"
            context.user_data["mode"] = "write_note_remove"

            await query.message.reply_text(
                f"🚛 Техника: {car}\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\n\n"
                "🔴 <b>Қилинган иш бўйича изоҳ ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        # REGISTRATION / REGISTRATION EDIT CAR
        if mode in ["driver_car", "driver_edit_car"]:
            firm = context.user_data.get("driver_firm")
            counts = get_driver_car_counts_by_firm(firm) if firm else {}
            if not is_car_available_for_driver(car, counts):
                await query.answer("❌ Бу техникага ҳайдовчи бириктирилган.", show_alert=True)
                await query.message.reply_text(
                    "❌ Бу техникага ҳайдовчи бириктирилган. Илтимос, бошқа техникани танланг.",
                    reply_markup=car_buttons_by_firm(firm, only_available_for_driver=True)
                )
                return

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["driver_car"] = car
            context.user_data["mode"] = "driver_confirm"

            await show_driver_confirm(query.message, context)
            return

        # STAFF EDIT CAR
        if mode == "technadzor_staff_edit_car":
            driver_id = context.user_data.get("staff_edit_driver_id")
            if driver_id:
                cursor.execute("UPDATE drivers SET car = %s WHERE telegram_id = %s", (car, driver_id))
                conn.commit()
                sync_driver_to_google_sheet(telegram_id=driver_id)
            context.user_data["mode"] = "technadzor_staff_card"
            await query.message.reply_text("✅ Техника ўзгартирилди.")
            return

    # === PRIORITY FIX END ===



    # === PRIORITY FIX: регистрация Тасдиқлаш/Таҳрирлаш ===
    if data == "edit_driver":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=register_edit_keyboard(context)
        )
        return

    if data == "confirm_driver":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        user_id = update.effective_user.id

        if context.user_data.get("driver_work_role") == "zapravshik":
            context.user_data["driver_firm"] = ""
            context.user_data["driver_car"] = ""

        # PostgreSQL — асосий база
        cursor.execute("""
            INSERT INTO drivers (
                telegram_id,
                name,
                surname,
                phone,
                firm,
                car,
                status,
                work_role
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                surname = EXCLUDED.surname,
                phone = EXCLUDED.phone,
                firm = EXCLUDED.firm,
                car = EXCLUDED.car,
                status = EXCLUDED.status,
                work_role = EXCLUDED.work_role
        """, (
            user_id,
            context.user_data.get("driver_name", ""),
            context.user_data.get("driver_surname", ""),
            context.user_data.get("phone", ""),
            context.user_data.get("driver_firm", ""),
            context.user_data.get("driver_car", ""),
            "Текширувда",
            context.user_data.get("driver_work_role", "driver")
        ))
        conn.commit()

        # Google Sheets — қўшимча, хато берса бот тўхтамасин
        try:
            drivers_ws.append_row([
                user_id,
                context.user_data.get("driver_name", ""),
                context.user_data.get("driver_surname", ""),
                context.user_data.get("phone", ""),
                context.user_data.get("driver_firm", ""),
                context.user_data.get("driver_car", ""),
                "Текширувда",
                now_text(),
                context.user_data.get("driver_work_role", "driver")
            ])
        except Exception as e:
            print(f"[registration] Google Sheets append_row error: {e}")

        # Технадзорга хабар
        try:
            surname = context.user_data.get("driver_surname", "")
            name = context.user_data.get("driver_name", "")

            for tech_id in get_user_ids_by_role("technadzor"):
                try:
                    await context.bot.send_message(
                        chat_id=tech_id,
                        text=(
                            "🔔 Уведомления\n"
                            "👤 Янги ходим\n"
                            f"{surname} {name}".strip()
                        )
                    )
                except Exception as e:
                    print(f"[registration] send technadzor error: {e}")
        except Exception as e:
            print(f"[registration] notify technadzor block error: {e}")

        await query.message.reply_text("✅ Рўйхатдан ўтдингиз. Текширувга юборилди.")
        context.user_data.clear()
        return

    # === PRIORITY FIX END ===

    # /start эски inline кнопкаларни блоклайди.
    # Лекин ҳозирги актив жараёндаги inline кнопкалар (регистрация/ремонт/ходимлар) ишлаши керак.
    current_mode = context.user_data.get("mode")
    active_inline_allowed = False

    # Актив жараёнлардаги inline кнопкалар /start дан кейин ҳам ишлаши керак.
    # Эски меню кнопкалари эса inline_disabled_by_start билан блокланади.
    if data in ["confirm_driver", "edit_driver"]:
        active_inline_allowed = True

    if data.startswith("driver_edit|"):
        active_inline_allowed = True

    if data.startswith("car_"):
        active_inline_allowed = True

    if data.startswith("car|"):
        active_inline_allowed = True

    # Регистрация inline кнопкалари doim ishlashi kerak
    if data.startswith("firm_") or data.startswith("role_") or data.startswith("register_"):
        active_inline_allowed = True

    # Ремонт inline кнопкалари doim ishlashi kerak
    if data.startswith("repair_") or data.startswith("remont_"):
        active_inline_allowed = True

    if data.startswith("approve_driver|") or data.startswith("reject_driver|"):
        active_inline_allowed = True

    if data.startswith("diesel_prihod_"):
        active_inline_allowed = True

    if data.startswith("tz_reg") or data.startswith("tz_reg_approve|") or data.startswith("tz_reg_reject|"):
        active_inline_allowed = current_mode in [
            "technadzor_registration_list",
            "technadzor_staff_card",
        ]

    if data.startswith("tz_staff"):
        active_inline_allowed = current_mode in [
            "technadzor_staff_menu",
            "technadzor_staff_drivers_firm",
            "technadzor_staff_drivers_list",
            "technadzor_staff_mechanics_list",
            "technadzor_staff_zapravshik_list",
            "technadzor_staff_card",
        ]

    if context.user_data.get("inline_disabled_by_start") and not active_inline_allowed:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.answer("Бу эски кнопка. /start дан кейин янги менюдан фойдаланинг.", show_alert=True)
        return

    if data == "tz_staff|drivers":
        await safe_edit_message_text(query, 
            "🚚 Ҳайдовчилар\n\nФирмани танланг:",
            reply_markup=technadzor_staff_firms_keyboard("drivers")
        )
        return

    if data == "tz_staff|mechanics":
        context.user_data["technadzor_staff_type"] = "mechanics"
        context.user_data.pop("technadzor_staff_firm", None)
        await safe_edit_message_text(query, 
            technadzor_staff_list_text("mechanics"),
            reply_markup=technadzor_staff_list_inline_keyboard("mechanics")
        )
        return

    if data == "tz_staff|zapravshik":
        context.user_data["technadzor_staff_type"] = "zapravshik"
        context.user_data.pop("technadzor_staff_firm", None)
        await safe_edit_message_text(query, 
            technadzor_staff_list_text("zapravshik"),
            reply_markup=technadzor_staff_list_inline_keyboard("zapravshik")
        )
        return

    if data.startswith("tz_staff_firm|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            await query.answer("Маълумот нотўғри.", show_alert=True)
            return

        staff_type = parts[1]
        firm = parts[2]
        context.user_data["technadzor_staff_type"] = staff_type
        context.user_data["technadzor_staff_firm"] = firm

        await safe_edit_message_text(query, 
            technadzor_staff_list_text(staff_type, firm),
            reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
        )
        return

    if data.startswith("tz_reg_view|"):
        driver_id = data.split("|", 1)[1]
        technadzor_clear_pending_decision_done(context, driver_id)
        context.user_data["mode"] = "technadzor_staff_card"
        context.user_data["technadzor_selected_staff_id"] = driver_id
        await safe_edit_message_text(query, 
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_pending_registration_card_keyboard(driver_id)
        )
        return

    if data.startswith("tz_staff_view|"):
        driver_id = data.split("|", 1)[1]
        context.user_data.setdefault("technadzor_staff_action_stack", []).append({
            "screen": "list",
            "staff_type": context.user_data.get("technadzor_staff_type", "drivers"),
            "firm": context.user_data.get("technadzor_staff_firm"),
        })
        context.user_data["mode"] = "technadzor_staff_card"
        context.user_data["technadzor_selected_staff_id"] = driver_id
        remember_staff_inline_query_message(context, query)
        await safe_edit_message_text(query, 
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        remember_staff_inline_query_message(context, query)
        return

    if data.startswith("tz_staff_edit|"):
        driver_id = data.split("|", 1)[1]
        technadzor_save_pending_edit_backup(context, driver_id)

        context.user_data.setdefault("technadzor_staff_action_stack", []).append({
            "screen": "card",
            "driver_id": driver_id,
        })
        context.user_data["mode"] = "technadzor_staff_edit_menu"
        context.user_data["technadzor_selected_staff_id"] = driver_id
        remember_staff_inline_query_message(context, query)
        await safe_edit_message_text(query, 
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=technadzor_staff_edit_keyboard(driver_id)
        )
        remember_staff_inline_query_message(context, query)
        return

    if data.startswith("tz_staff_edit_field|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            await query.answer("Маълумот нотўғри.", show_alert=True)
            return

        driver_id = parts[1]
        field = parts[2]
        technadzor_save_pending_edit_backup(context, driver_id)

        context.user_data.setdefault("technadzor_staff_action_stack", []).append({
            "screen": "edit_menu",
            "driver_id": driver_id,
        })
        context.user_data["technadzor_selected_staff_id"] = driver_id
        context.user_data["technadzor_staff_edit_field"] = field

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        if field == "name":
            context.user_data["mode"] = "technadzor_staff_edit_name"
            await query.message.reply_text("👤 Янги исмни киритинг:", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        if field == "surname":
            context.user_data["mode"] = "technadzor_staff_edit_surname"
            await query.message.reply_text("👤 Янги фамилияни киритинг:", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        if field == "phone":
            context.user_data["mode"] = "technadzor_staff_edit_phone"
            await query.message.reply_text("📞 Янги телефон рақамни юборинг:", reply_markup=phone_back_keyboard())
            return

        if field == "role":
            context.user_data["mode"] = "technadzor_staff_edit_role"
            await query.message.reply_text("🪪 Янги лавозимни танланг:", reply_markup=technadzor_staff_role_reply_keyboard())
            return

        if field == "firm":
            staff = get_staff_by_id(driver_id)
            if staff and staff.get("work_role") == "zapravshik":
                await query.answer("Заправщик барча фирмаларга тегишли.", show_alert=True)
                return
            context.user_data["mode"] = "technadzor_staff_edit_firm"
            await query.message.reply_text("🏢 Янги фирмани танланг:", reply_markup=technadzor_staff_firms_reply_keyboard())
            return

        if field == "car":
            staff = get_staff_by_id(driver_id)
            if not staff or staff.get("work_role") != "driver":
                await query.answer("Техника фақат ҳайдовчи учун танланади.", show_alert=True)
                return
            firm = staff["firm"] if staff else ""
            if not firm:
                await query.message.reply_text("❌ Аввал фирмани танланг.", reply_markup=technadzor_staff_firms_reply_keyboard())
                context.user_data["mode"] = "technadzor_staff_edit_firm"
                return
            context.user_data["mode"] = "technadzor_staff_edit_car"
            await query.message.reply_text("🚛 Янги техникани танланг:", reply_markup=technadzor_staff_cars_reply_keyboard(firm, exclude_driver_id=driver_id))
            return

    if data.startswith("tz_staff_delete|"):
        driver_id = data.split("|", 1)[1]
        staff = get_staff_by_id(driver_id)

        if not staff:
            await query.answer("Ходим топилмади.", show_alert=True)
            return

        telegram_id = staff.get("telegram_id")
        staff_type = get_staff_type_from_work_role(staff.get("work_role"))
        firm = staff.get("firm") if staff_type == "drivers" else None

        try:
            cursor.execute("DELETE FROM drivers WHERE id = %s", (int(driver_id),))
            conn.commit()
            delete_driver_from_google_sheet(telegram_id)
        except Exception as e:
            print("TECHNADZOR STAFF DELETE ERROR:", e)
            await query.answer("❌ Ўчиришда хато.", show_alert=True)
            return

        try:
            await clear_blocked_user_bot_messages(context, telegram_id)
        except Exception:
            pass

        try:
            await context.bot.send_message(
                chat_id=int(telegram_id),
                text="🗑 Сизнинг маълумотларингиз тизимдан ўчирилди.",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception:
            pass

        context.user_data["technadzor_staff_type"] = staff_type
        context.user_data["technadzor_staff_firm"] = firm

        await safe_edit_message_text(query, 
            "🗑 Ходим PostgreSQL ва Google Sheetsдан ўчирилди.\n\n"
            + technadzor_staff_list_text(staff_type, firm),
            reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
        )
        return

    if data.startswith("tz_staff_status|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            await query.answer("Маълумот нотўғри.", show_alert=True)
            return

        driver_id = parts[1]
        new_label = parts[2]
        new_status = staff_status_db(new_label)
        staff = get_staff_by_id(driver_id)

        if not staff:
            await query.answer("Ходим топилмади.", show_alert=True)
            return

        telegram_id = staff.get("telegram_id")

        try:
            cursor.execute("UPDATE drivers SET status = %s WHERE id = %s", (new_status, int(driver_id)))
            conn.commit()
            sync_driver_to_google_sheet(driver_id=driver_id)
        except Exception as e:
            print("TECHNADZOR STAFF STATUS UPDATE ERROR:", e)
            await query.answer("❌ Сақлашда хато.", show_alert=True)
            return

        if new_status == "Рад этилди":
            try:
                await clear_blocked_user_bot_messages(context, telegram_id)
            except Exception:
                pass
            await notify_staff_blocked(context, telegram_id)
        elif new_status == "Тасдиқланди":
            await notify_staff_play(context, telegram_id)

        context.user_data["mode"] = "technadzor_staff_card"
        context.user_data["technadzor_selected_staff_id"] = driver_id
        remember_staff_inline_query_message(context, query)
        await safe_edit_message_text(query, 
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        remember_staff_inline_query_message(context, query)
        return

    if data == "tz_staff_action_back":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        staff = get_staff_by_id(driver_id) if driver_id else None

        # Inline орқага фақат битта амал орқага қайтаради.
        # Бу ерда rollback қилинмайди.
        if staff and staff.get("status") == "Текширувда":
            await safe_edit_message_text(query, 
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_pending_registration_card_keyboard(driver_id)
            )
            return

        stack = context.user_data.get("technadzor_staff_action_stack", [])
        last = stack.pop() if stack else None
        context.user_data["technadzor_staff_action_stack"] = stack

        if not last:
            staff_type = context.user_data.get("technadzor_staff_type", "drivers")
            firm = context.user_data.get("technadzor_staff_firm")
            context.user_data["mode"] = "technadzor_staff_drivers_list" if staff_type == "drivers" else f"technadzor_staff_{staff_type}_list"
            remember_staff_inline_query_message(context, query)
            await safe_edit_message_text(query, 
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            remember_staff_inline_query_message(context, query)
            return

        if last.get("screen") == "list":
            staff_type = last.get("staff_type", "drivers")
            firm = last.get("firm")
            context.user_data["mode"] = "technadzor_staff_drivers_list" if staff_type == "drivers" else f"technadzor_staff_{staff_type}_list"
            remember_staff_inline_query_message(context, query)
            await safe_edit_message_text(query, 
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            remember_staff_inline_query_message(context, query)
            return

        if last.get("screen") == "card":
            driver_id = last.get("driver_id")
            context.user_data["mode"] = "technadzor_staff_card"
            context.user_data["technadzor_selected_staff_id"] = driver_id
            remember_staff_inline_query_message(context, query)
            await safe_edit_message_text(query, 
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            remember_staff_inline_query_message(context, query)
            return

        if last.get("screen") == "edit_menu":
            driver_id = last.get("driver_id")
            staff = get_staff_by_id(driver_id)
            if staff and staff.get("status") == "Текширувда":
                await safe_edit_message_text(query, 
                    technadzor_staff_card_text(driver_id),
                    reply_markup=technadzor_pending_registration_card_keyboard(driver_id)
                )
                return

            context.user_data["mode"] = "technadzor_staff_edit_menu"
            remember_staff_inline_query_message(context, query)
            await safe_edit_message_text(query, 
                "✏️ Қайси маълумотни таҳрирлайсиз?",
                reply_markup=technadzor_staff_edit_keyboard(driver_id)
            )
            remember_staff_inline_query_message(context, query)
            return

    if data.startswith("tz_staff_back|"):
        back_to = data.split("|", 1)[1]

        if back_to == "staff_menu":
            await safe_edit_message_text(query, 
                "👥 Ходимлар менюси",
                reply_markup=technadzor_staff_menu_inline_keyboard()
            )
            return

        if back_to == "drivers_firms":
            await safe_edit_message_text(query, 
                "🚚 Ҳайдовчилар\n\nФирмани танланг:",
                reply_markup=technadzor_staff_firms_keyboard("drivers")
            )
            return

    if data == "fuel_gas_view":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(fuel_gas_confirm_text(context))

        video_id = context.user_data.get("fuel_video_id")
        photo_id = context.user_data.get("fuel_photo_id")

        if video_id:
            await safe_send_video(context.bot, query.message.chat_id, video_id)

        if photo_id:
            await safe_send_photo(context.bot, query.message.chat_id, photo_id)

        await query.message.reply_text(
            "Маълумотни тасдиқлайсизми?",
            reply_markup=fuel_gas_after_action_keyboard()
        )
        return

    if data == "fuel_gas_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=fuel_gas_edit_keyboard()
        )
        return

    if data == "fuel_gas_edit_km":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        context.user_data["mode"] = "fuel_gas_edit_km"
        await query.message.reply_text(
            "📍 Янги спидометр кўрсаткичини киритинг.\n\nМисол: 15300"
        )
        return

    if data == "fuel_gas_edit_video":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        context.user_data["mode"] = "fuel_gas_edit_video"
        await query.message.reply_text(
            "🎥 Янги видео юборинг.\n\nАвтомобил рақами ва калонка кўрсаткичи кўринсин."
        )
        return

    if data == "fuel_gas_edit_photo":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        context.user_data["mode"] = "fuel_gas_edit_photo"
        await query.message.reply_text(
            "📷 Янги ведомость расмини юборинг."
        )
        return

    if data == "fuel_gas_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        context.user_data.clear()
        context.user_data["mode"] = "fuel_menu"
        await query.message.reply_text(
            "❌ Газ олиш маълумоти бекор қилинди.",
            reply_markup=gas_report_keyboard()
        )
        return

    if data == "fuel_gas_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        user_id = update.effective_user.id

        cursor.execute("""
            INSERT INTO fuel_reports (
                telegram_id,
                car,
                fuel_type,
                km,
                video_id,
                photo_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            context.user_data.get("fuel_car"),
            context.user_data.get("fuel_type"),
            context.user_data.get("fuel_km"),
            context.user_data.get("fuel_video_id"),
            context.user_data.get("fuel_photo_id")
        ))

        conn.commit()

        context.user_data.clear()
        context.user_data["mode"] = "fuel_menu"

        await query.message.reply_text(
            "✅ Газ олиш маълумоти базага сақланди.",
            reply_markup=gas_report_keyboard()
        )
        return

    if data == "gasgive_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        cancel_gas_auto_confirm_task(context)

        gas_transfer_id = context.user_data.get("gasgive_transfer_id")
        if gas_transfer_id:
            cursor.execute("DELETE FROM gas_transfers WHERE id = %s", (gas_transfer_id,))
            conn.commit()

        context.user_data.clear()
        context.user_data["mode"] = "fuel_menu"

        await query.message.reply_text(
            "❌ Газ бериш маълумоти бекор қилинди.",
            reply_markup=gas_report_keyboard()
        )
        return

    if data == "gasgive_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        cancel_gas_auto_confirm_task(context)

        if context.user_data.get("gasgive_sent"):
            await query.message.reply_text("✅ Бу маълумот аллақачон тасдиқланган.")
            return

        context.user_data["gasgive_sent"] = True

        from_car = context.user_data.get("gasgive_from_car")
        to_car = context.user_data.get("gasgive_to_car")
        firm = context.user_data.get("gasgive_firm")
        note = context.user_data.get("gasgive_note")
        video_id = context.user_data.get("gasgive_video_id")
        user_id = update.effective_user.id

        receiver = get_driver_by_car(to_car)

        if receiver:
            to_driver_id = receiver[0]
            transfer_status = "Қабул қилувчи текширувида"
            answered_sql = "NULL"
        else:
            to_driver_id = None
            transfer_status = "Тасдиқланди"
            answered_sql = "NOW()"

        cursor.execute(f"""
            INSERT INTO gas_transfers (
                from_driver_id,
                from_car,
                to_driver_id,
                to_car,
                firm,
                note,
                video_id,
                status,
                answered_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, {answered_sql})
            RETURNING id
        """, (
            user_id,
            from_car,
            to_driver_id,
            to_car,
            firm,
            note,
            video_id,
            transfer_status
        ))

        transfer_id = cursor.fetchone()[0]
        context.user_data["gasgive_transfer_id"] = transfer_id
        conn.commit()

        if receiver:
            await send_gas_transfer_to_receiver(context, transfer_id)
            result_text = "✅ Газ бериш маълумоти базага сақланди ва олувчи ҳайдовчига юборилди."
        else:
            result_text = (
                "✅ Газ бериш маълумоти базага сақланди.\n"
                "🚛 Газ олувчи техникада ҳайдовчи йўқлиги учун маълумот автоматик қабул қилинди."
            )

        await query.message.reply_text(
            result_text,
            reply_markup=gas_report_keyboard()
        )

        context.user_data.clear()
        context.user_data["mode"] = "fuel_menu"
        return

    if data == "gasgive_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "gasgive_edit_menu"

        schedule_gas_auto_confirm_task(context, update.effective_user.id)

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=gas_give_edit_keyboard()
        )
        return

    if data == "gasgive_edit_to_car":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["gasgive_note_before_edit"] = context.user_data.get("gasgive_note") or ""
        context.user_data["mode"] = "gasgive_edit_firm"

        schedule_gas_auto_confirm_task(context, update.effective_user.id)

        await query.message.reply_text(
            "🏢 Қайси фирмадаги газли техникани танлайсиз?",
            reply_markup=gas_firm_keyboard()
        )
        return

    if data == "gasgive_edit_note":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "gasgive_edit_note_text"

        await query.message.reply_text(
            "🔴 Янги изоҳни киритинг.\n\nФақат ҳарф ва рақам ёзиш мумкин."
        )
        return

    if data == "gasgive_edit_video":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "gasgive_video"

        await query.message.reply_text(
            "🎥 Янги думалоқ видео юборинг.\n\n⏱ Видео 10 сониядан кам бўлмасин."
        )
        return

    if data.startswith("gas_rejected_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_car,
                firm,
                note,
                video_id,
                receiver_comment,
                created_at
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_car, firm, note, video_id, receiver_comment, created_at = row

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        reject_reason = receiver_comment or "Кўрсатилмаган"
        note = note or ""

        if video_id:
            await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.reply_text(
            "❌ ГАЗ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Газ берган техника: {from_car}\n"
            f"🚛 Газ олган техника: {to_car}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reject_reason}\n"
            "📌 Статус: Рад этилди\n\n"
            "Маълумотни нима қиласиз?",
            reply_markup=gas_rejected_after_view_keyboard(transfer_id)
        )
        return

    if data.startswith("gas_rejected_resend|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id = row[0]

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        cursor.execute("""
            SELECT to_car
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))
        to_car_row = cursor.fetchone()
        current_to_car = to_car_row[0] if to_car_row else None
        receiver = get_driver_by_car(current_to_car)

        if receiver:
            cursor.execute("""
                UPDATE gas_transfers
                SET to_driver_id = %s,
                    status = %s,
                    receiver_comment = NULL,
                    answered_at = NULL
                WHERE id = %s
            """, (receiver[0], "Қабул қилувчи текширувида", transfer_id))
            conn.commit()
            await send_gas_transfer_to_receiver(context, transfer_id)
            await query.message.reply_text("✅ Маълумот қайта газ олувчига юборилди.", reply_markup=gas_report_keyboard())
        else:
            cursor.execute("""
                UPDATE gas_transfers
                SET to_driver_id = NULL,
                    status = %s,
                    receiver_comment = NULL,
                    answered_at = NOW()
                WHERE id = %s
            """, ("Тасдиқланди", transfer_id))
            conn.commit()
            await query.message.reply_text(
                "✅ Газ олувчи техникада ҳайдовчи йўқлиги учун маълумот автоматик қабул қилинди.",
                reply_markup=gas_report_keyboard()
            )
        return

    if data.startswith("gas_rejected_edit|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_car,
                firm,
                note,
                video_id
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_car, firm, note, video_id = row

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        context.user_data["gas_edit_rejected_id"] = transfer_id
        context.user_data["gasgive_from_car"] = from_car or ""
        context.user_data["gasgive_to_car"] = to_car or ""
        context.user_data["gasgive_firm"] = firm or ""
        context.user_data["gasgive_note"] = note or ""
        context.user_data["gasgive_video_id"] = video_id or ""
        context.user_data["mode"] = "gasgive_edit_menu"

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=gas_give_edit_keyboard()
        )
        return

    if data.startswith("gas_rejected_cancel|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id = row[0]

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        cursor.execute("""
            UPDATE gas_transfers
            SET status = %s,
                answered_at = NOW()
            WHERE id = %s
        """, ("Бекор қилинди", transfer_id))

        conn.commit()

        await query.message.reply_text("❌ Рад этилган газ маълумоти бекор қилинди.", reply_markup=gas_report_keyboard())
        return

    if data.startswith("gas_receive_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at
            FROM gas_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, note, video_id, created_at = row

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        note = note or ""

        text = (
            "⛽ ГАЗ МАЪЛУМОТИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Газ берган техника: {from_car}\n"
            f"🚛 Газ олган техника: {to_car}\n"
            f"📝 Изоҳ: {note}\n"
        )

        if video_id:
            try:
                await context.bot.send_video_note(
                    chat_id=query.message.chat_id,
                    video_note=video_id
                )
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.reply_text(
            text,
            reply_markup=gas_receiver_after_view_keyboard(transfer_id)
        )
        return

    if data.startswith("gasgive_accept|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            UPDATE gas_transfers
            SET status = %s, answered_at = NOW()
            WHERE id = %s
            RETURNING from_driver_id
        """, ("Тасдиқланди", int(update.effective_user.id), transfer_id))

        row = cursor.fetchone()
        conn.commit()

        if row:
            await notify_gas_sender_confirmed(context, transfer_id)

        await query.message.reply_text("✅ Маълумот тасдиқланди.")
        return

    if data.startswith("gasgive_reject|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        context.user_data["mode"] = "gasgive_receiver_reject_note"
        context.user_data["gasgive_reject_transfer_id"] = transfer_id

        await query.message.reply_text(
            "❌ Рад этиш сабабини ёзинг.\n\n"
            "Фақат ҳарф ва рақам қабул қилинади.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data.startswith("gasgive_edit_car|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = data.split("|", 1)[1]

        context.user_data["gasgive_to_car"] = car
        if not context.user_data.get("gasgive_note"):
            context.user_data["gasgive_note"] = context.user_data.get("gasgive_note_before_edit", "")
        context.user_data["mode"] = "gasgive_confirm"

        to_driver = get_driver_by_car(car)
        context.user_data["gasgive_to_driver_name"] = short_driver_name(to_driver)

        sent_msg = await query.message.reply_text(
            gas_confirm_text(context),
            reply_markup=gas_give_confirm_keyboard()
        )

        context.user_data["gasgive_confirm_message_id"] = sent_msg.message_id

        schedule_gas_auto_confirm_task(context, update.effective_user.id)

        return

    if data.startswith("gasgive_car|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = data.split("|", 1)[1]

        context.user_data["gasgive_to_car"] = car
        context.user_data["mode"] = "gasgive_note"

        await query.message.reply_text(
            f"🚛 ГАЗ оладиган техника: {car}\n\n"
            "🔴 Нега ГАЗ беряпсиз? Изоҳ ёзинг!\n\n"
            "Фақат ҳарф ва рақам ёзиш мумкин.",
            reply_markup=back_keyboard()
        )
        return

        to_driver_id = receiver[0]

        cursor.execute("""
            INSERT INTO gas_transfers (
                from_driver_id,
                from_car,
                to_driver_id,
                to_car,
                firm,
                note,
                video_id,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            user_id,
            from_car,
            to_driver_id,
            to_car,
            firm,
            note,
            video_id,
            "Қабул қилувчи текширувида"
        ))

        transfer_id = cursor.fetchone()[0]
        context.user_data["gasgive_transfer_id"] = transfer_id
        conn.commit()

        await send_gas_transfer_to_receiver(context, transfer_id)

        await query.message.reply_text(
            "✅ Газ бериш маълумоти базага сақланди ва олувчи ҳайдовчига юборилди.",
            reply_markup=gas_report_keyboard()
        )

        context.user_data.clear()
        context.user_data["mode"] = "fuel_menu"
        return

    if data == "dieselgive_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data.clear()
        context.user_data["mode"] = "diesel_menu"

        await query.message.reply_text(
            "❌ Дизел маълумоти бекор қилинди.",
            reply_markup=diesel_report_keyboard(update.effective_user.id)
        )
        return

    if data == "dieselgive_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_menu"

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=diesel_give_edit_keyboard()
        )
        return

    if data == "diesel_edit_car":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_firm"

        await query.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=(
                diesel_prihod_firm_stock_keyboard()
                if is_zapravshik_diesel_expense_flow(context)
                else diesel_firm_plain_keyboard()
            )
        )
        return

    if data == "diesel_edit_liter":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_liter"

        await query.message.reply_text(
            "⛽ Янги дизел миқдорини киритинг.\n\n"
            "Фақат рақам киритинг.\n"
            "Мисол: 60",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_note":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_note"

        await query.message.reply_text(
            "📝 Янги изоҳни киритинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_speed_photo":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_speed_photo"

        await query.message.reply_text(
            "📍 Янги спидометр ёки моточас кўрсаткичини ёзинг.\n\n"
            "❌ Фақат сон қабул қилинади.\n"
            "Мисол: 125000",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_speed_photo":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_speed_photo"

        await query.message.reply_text(
            "📍 Янги спидометр ёки моточас кўрсаткичини ёзинг.\n\n"
            "❌ Фақат сон қабул қилинади.\n"
            "Мисол: 125000",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_video":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_video"

        await query.message.reply_text(
            "🎥 Янги думалоқ видео юборинг.\n\n"
            "⏱ Видео 10 сониядан кам бўлмасин.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # === EARLY PATCH: HISTORY PERIOD AND DIESEL VIEW START ===

    if data.startswith("period|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        period = data.split("|", 1)[1]

        if period == "custom":
            context.user_data["mode"] = "history_custom_period"

            await query.message.reply_text(
                "🔴 <b>Қайси давр оралиғини кўрмоқчисиз?</b>\n\n"
                "Мисол:\n"
                "01.01.2026-28.04.2026",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        car = context.user_data.get("history_car")

        if not car:
            await query.message.reply_text("❌ Техника танланмаган. Историяни қайтадан бошланг.")
            return

        start_date, end_date = get_period_dates(period)

        if not start_date or not end_date:
            await query.message.reply_text("❌ Давр хатолик.")
            return

        await send_history_by_date(query.message, car, start_date, end_date)

        context.user_data["mode"] = None
        return

    if data.startswith("diesel_rejected_view|"):
        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_car,
                firm,
                liter,
                note,
                video_id,
                receiver_comment,
                created_at
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_car, firm, liter, note, video_id, receiver_comment, created_at = row

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        reject_reason = receiver_comment or "Кўрсатилмаган"

        await query.message.chat.send_message(
            "❌ ДИЗЕЛ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n"
            f"📌 Статус: Қайтарилди\n"
            f"❗ Рад этилиш сабаби: {reject_reason}\n\n"
            "Маълумотни нима қиласиз?"
        )

        if video_id:
            await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.chat.send_message(
            "Маълумотни нима қиласиз?",
            reply_markup=diesel_rejected_after_view_keyboard(transfer_id)
        )
        return

    if data.startswith("diesel_rejected_edit|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
    
        transfer_id = data.split("|", 1)[1]
    
        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_car,
                firm,
                liter,
                note,
                speedometer_photo_id,
                video_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))
    
        row = cursor.fetchone()
    
        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return
    
        from_driver_id, from_car, to_car, firm, liter, note, speedometer_photo_id, video_id = row
    
        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return
    
        context.user_data["diesel_edit_rejected_id"] = transfer_id
        context.user_data["dieselgive_from_car"] = from_car or ""
        context.user_data["dieselgive_to_car"] = to_car or ""
        context.user_data["dieselgive_firm"] = firm or ""
        context.user_data["dieselgive_liter"] = liter or ""
        context.user_data["dieselgive_note"] = note or ""
        context.user_data["dieselgive_speedometer_photo_id"] = speedometer_photo_id or ""
        context.user_data["dieselgive_video_id"] = video_id or ""
        context.user_data["mode"] = "dieselgive_edit_menu"
    
        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=diesel_give_edit_keyboard()
        )
        return

    if data.startswith("diesel_receiver_rejected_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT
                to_driver_id,
                from_driver_id,
                from_car,
                to_car,
                firm,
                liter,
                note,
                video_id,
                receiver_comment,
                created_at
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        to_driver_id, from_driver_id, from_car, to_car, firm, liter, note, video_id, receiver_comment, created_at = row

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        reject_reason = receiver_comment or "Кўрсатилмаган"

        if video_id:
            await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.reply_text(
            "❌ ДИЗЕЛ ОЛИШ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган: {to_car}\n"
            f"📝 Изоҳ: {note}\n"
            f"⛽ Литр: {liter}\n\n"
            f"❗ Рад этилиш сабаби: {reject_reason}\n\n"
            "Маълумотни нима қиласиз?",
            reply_markup=diesel_rejected_receiver_keyboard(transfer_id)
        )
        return

    # === EARLY PATCH: HISTORY PERIOD AND DIESEL VIEW END ===

    if query.data == "none":
        return

        # === PRIORITY CAR CALLBACK FIX START ===

    if data.startswith("dieselgive_car|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = data.split("|", 1)[1]

        context.user_data["dieselgive_to_car"] = car

        if context.user_data.get("mode") == "dieselgive_edit_car":
            context.user_data["mode"] = "dieselgive_confirm"

            await query.message.reply_text(
                diesel_confirm_text(context),
                reply_markup=diesel_give_final_keyboard()
            )
            return

        context.user_data["mode"] = "dieselgive_liter"

        await query.message.reply_text(
            f"🚛 ДИЗЕЛ оладиган техника: {car}\n\n"
            "⛽ Неччи литр ДИЗЕЛ беряпсиз?\n\n"
            "Фақат рақам киритинг.\n"
            "Мисол: 60",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data.startswith("car|"):
        car = data.split("|", 1)[1]
        mode = context.user_data.get("mode")

        if mode == "history_select_car":
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["history_car"] = car
            context.user_data["mode"] = "history_period"

            msg = await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
            remember_inline_message(context, msg)
            return

    # === PRIORITY CAR CALLBACK FIX END ===

    if query.data == "final_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
    
    
        await save_final_data(update, context, query.message)
    
        await query.answer("Сақланди ✅")
        return

    if data.startswith("view_media|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        media_key = data.split("|", 1)[1]

        if media_key.startswith("tech_enter_"):
            row_id = media_key.replace("tech_enter_", "")
        
            rows = remont_ws.get_all_values()[1:]
        
            for row in rows:
                if len(row) < 9:
                    continue
        
                if str(row[0]).strip() != str(row_id).strip():
                    continue
        
                car = row[2] if len(row) > 2 else ""
                km = row[3] if len(row) > 3 else ""
                repair_type = row[4] if len(row) > 4 else ""
                note = row[6] if len(row) > 6 else ""
                video_id = row[7] if len(row) > 7 else ""
                photo_id = row[8] if len(row) > 8 else ""
                person = row[9] if len(row) > 9 else ""
                sana = row[10] if len(row) > 10 else row[1]
        
                await query.message.reply_text(
                    "🔴 РЕМОНТГА КИРГАН\n\n"
                    f"🚛 Техника: {car}\n"
                    f"📅 Сана: {sana}\n"
                    f"⏱ КМ/Моточас: {km}\n"
                    f"🔧 Ремонт тури: {repair_type}\n"
                    f"📝 Изоҳ: {clean_note(note)}\n"
                    f"👤 Киритган: {person}"
                )
        
                if photo_id:
                    await safe_send_photo(context.bot, query.message.chat_id, photo_id)
        
                if video_id:
                    await safe_send_video(context.bot, query.message.chat_id, video_id)
        
                return
        
            await query.message.reply_text("❌ Медиа топилмади.")
            return
        
        
        if media_key.startswith("tech_exit_"):
            row_id = media_key.replace("tech_exit_", "")
        
            rows = remont_ws.get_all_values()[1:]
        
            for row in rows:
                if len(row) < 8:
                    continue
        
                if str(row[0]).strip() != str(row_id).strip():
                    continue
        
                car = row[2] if len(row) > 2 else ""
                note = row[6] if len(row) > 6 else ""
                video_id = row[7] if len(row) > 7 else ""
                person = row[9] if len(row) > 9 else ""
                sana = row[11] if len(row) > 11 else row[1]
        
                await query.message.reply_text(
                    "🟡 РЕМОНТДАН ЧИҚҚАН\n\n"
                    f"🚛 Техника: {car}\n"
                    f"📅 Сана: {sana}\n"
                    f"📝 Изоҳ: {clean_note(note)}\n"
                    f"👤 Чиқарган: {person}"
                )
        
                if video_id:
                    await safe_send_video(context.bot, query.message.chat_id, video_id)
        
                return
        
            await query.message.reply_text("❌ Медиа топилмади.")
            return

        if media_key.startswith("history_"):
            parts = media_key.split("_", 2)

            if len(parts) < 3:
                return

            car = parts[1]
            date_text = parts[2]

            rows = remont_ws.get_all_values()[1:]

            for row in rows:
                if len(row) < 8:
                    continue

                row_date = row[1].strip()
                row_car = row[2].strip()
                video_id = row[7].strip()

                if row_car == car and row_date == date_text:
                    await query.message.reply_text(
                        f"🚛 {row_car}\n📅 {row_date}"
                    )

                    if video_id:
                        await safe_send_video(
                            context.bot,
                            query.message.chat_id,
                            video_id
                        )

                    return

        if media_key.startswith("gas_receiver_"):
            transfer_id = media_key.replace("gas_receiver_", "")

            cursor.execute("""
                SELECT from_car, to_car, firm, note, video_id, created_at
                FROM gas_transfers
                WHERE id = %s
            """, (transfer_id,))

            row = cursor.fetchone()

            if not row:
                await query.message.reply_text("❌ Медиа топилмади.")
                return


        if media_key.startswith("history_enter_"):
            repair_id = media_key.replace("history_enter_", "")

            cursor.execute("""
                SELECT car_number, km, repair_type, status, comment,
                       enter_photo, enter_video, entered_by, entered_at
                FROM repairs
                WHERE id = %s
            """, (repair_id,))

            row = cursor.fetchone()

            if not row:
                await query.message.reply_text("❌ Медиа топилмади.")
                return

            car_number, km, repair_type, status, comment, photo_id, video_id, entered_by, entered_at = row

            if entered_at:
                entered_at = entered_at.strftime("%d.%m.%Y %H:%M")

            await query.message.reply_text(
                f"🔴 Ремонтга кирган\n\n"
                f"🚘 {car_number}\n"
                f"📅 Сана: {entered_at}\n"
                f"📟 KM/Моточас: {km}\n"
                f"🛠 Ремонт: {repair_type}\n"
                f"📌 Статус: {status}\n"
                f"💬 Изоҳ: {comment}\n"
                f"👨‍🔧 Киритган: {entered_by}"
            )

            if photo_id:
                await safe_send_photo(context.bot, query.message.chat_id, photo_id)
        
            if video_id:
                await safe_send_video(context.bot, query.message.chat_id, video_id)
        
            return
        
        
        if media_key.startswith("history_exit_"):
            repair_id = media_key.replace("history_exit_", "")
        
            cursor.execute("""
                SELECT car_number, comment, enter_video, exited_by, exited_at
                FROM repairs
                WHERE id = %s
            """, (repair_id,))
        
            row = cursor.fetchone()
        
            if not row:
                await query.message.reply_text("❌ Медиа топилмади.")
                return
        
            car_number, comment, video_id, exited_by, exited_at = row
        
            if exited_at:
                exited_at = exited_at.strftime("%d.%m.%Y %H:%M")
        
            await query.message.reply_text(
                f"🟢 Ремонтдан чиққан\n\n"
                f"🚘 {car_number}\n"
                f"📅 Сана: {exited_at}\n"
                f"💬 Изоҳ: {comment}\n"
                f"👨‍🔧 Чиқарган: {exited_by}"
            )
        
            if video_id:
                await safe_send_video(context.bot, query.message.chat_id, video_id)
        
            return

            from_car, to_car, firm, note, video_id, created_at = row

            from_driver = get_driver_by_car(from_car)
            to_driver = get_driver_by_car(to_car)

            from_driver_name = short_driver_name(from_driver)
            to_driver_name = short_driver_name(to_driver)

            if created_at:
                created_at = created_at.strftime("%d.%m.%Y %H:%M")

            await query.message.reply_text(
                "⛽ ГАЗ бериш маълумоти\n\n"
                f"🕒 Вақт: {created_at}\n"
                f"🏢 Фирма: {firm}\n"
                f"🚛 Газ берувчи: {from_car} — {from_driver_name}\n"
                f"🚛 Газ олувчи: {to_car} — {to_driver_name}\n"
                f"📝 Изоҳ: {note}"
            )

            if video_id:
                await safe_send_video(context.bot, query.message.chat_id, video_id)

            return

    if data == "dieselgive_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        user_id = update.effective_user.id
        from_car = context.user_data.get("dieselgive_from_car")
        to_car = context.user_data.get("dieselgive_to_car")
        firm = context.user_data.get("dieselgive_firm")
        liter = context.user_data.get("dieselgive_liter")

        if is_zapravshik_diesel_expense_flow(context):
            can_spend, total_stock, spend = can_spend_diesel_amount(liter)
            if not can_spend:
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass

                await query.message.reply_text(
                    "❌ Дизел остаткаси етарли эмас.\n\n"
                    f"Жами фирмалар остаткаси: {format_liter(total_stock)} л\n"
                    f"Сиз киритган расход: {format_liter(spend)} л",
                    reply_markup=diesel_report_keyboard(update.effective_user.id)
                )
                return
        note = context.user_data.get("dieselgive_note")
        speedometer_photo_id = context.user_data.get("dieselgive_speedometer_photo_id")
        video_id = context.user_data.get("dieselgive_video_id")

        rejected_transfer_id = context.user_data.get("diesel_edit_rejected_id")

        if rejected_transfer_id:
            cursor.execute("""
                SELECT from_driver_id
                FROM diesel_transfers
                WHERE id = %s
            """, (rejected_transfer_id,))

            row = cursor.fetchone()

            if not row:
                await query.message.reply_text("❌ Рад этилган маълумот топилмади.")
                return

            old_from_driver_id = row[0]

            if int(update.effective_user.id) != int(old_from_driver_id):
                await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
                return

            receiver = get_driver_by_car(to_car)

            if receiver:
                to_driver_id = receiver[0]
                resend_auto_approved = False
            else:
                to_driver_id = None
                resend_auto_approved = True


            cursor.execute("""
                UPDATE diesel_transfers
                SET from_car = %s,
                    to_driver_id = %s,
                    to_car = %s,
                    firm = %s,
                    liter = %s,
                    note = %s,
                    speedometer_photo_id = %s,
                    video_id = %s,
                    status = %s,
                    receiver_comment = NULL,
                    answered_at = CASE WHEN %s THEN NOW() ELSE NULL END
                WHERE id = %s
            """, (
                from_car,
                to_driver_id,
                to_car,
                firm,
                liter,
                note,
                speedometer_photo_id,
                video_id,
                "Тасдиқланди" if resend_auto_approved else "Қабул қилувчи текширувида",
                resend_auto_approved,
                rejected_transfer_id
            ))

            conn.commit()

            if resend_auto_approved:
                await query.message.reply_text(
                    "✅ Таҳрирланган дизел маълумоти автоматик қабул қилинди.\n"
                    "Сабаб: ушбу техникага ҳайдовчи бириктирилмаган.",
                    reply_markup=diesel_report_keyboard(update.effective_user.id)
                )
            else:
                try:
                    await send_diesel_transfer_to_receiver(context, rejected_transfer_id)
                except Exception as e:
                    print("REJECTED DIESEL RESEND ERROR:", e)

                await query.message.reply_text(
                    "✅ Таҳрирланган дизел маълумоти қайта олувчи ҳайдовчига юборилди.",
                    reply_markup=diesel_report_keyboard(update.effective_user.id)
                )

            context.user_data.clear()
            context.user_data["mode"] = "diesel_menu"
            return

        receiver = get_driver_by_car(to_car)

        if receiver:
            to_driver_id = receiver[0]
            transfer_status = "Қабул қилувчи текширувида"
            auto_approved = False
        else:
            to_driver_id = None
            transfer_status = "Тасдиқланди"
            auto_approved = True

        cursor.execute("""
            INSERT INTO diesel_transfers (
                from_driver_id,
                from_car,
                to_driver_id,
                to_car,
                firm,
                liter,
                note,
                speedometer_photo_id,
                video_id,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            user_id,
            from_car,
            to_driver_id,
            to_car,
            firm,
            liter,
            note,
            speedometer_photo_id,
            video_id,
            transfer_status
        ))

        transfer_id = cursor.fetchone()[0]
        conn.commit()

        if auto_approved:
            await query.message.reply_text(
                "✅ Дизел расход автоматик қабул қилинди.\n"
                "📌 Статус: Қабул қилинди\n"
                "Сабаб: ушбу техникага ҳайдовчи бириктирилмаган.",
                reply_markup=diesel_report_keyboard(update.effective_user.id)
            )
        else:
            await send_diesel_transfer_to_receiver(context, transfer_id)

            await query.message.reply_text(
                "✅ ДИЗЕЛ расход маълумоти сақланди ва олувчи ҳайдовчига юборилди.\n📌 Статус: Қабул қилувчида",
                reply_markup=diesel_report_keyboard(update.effective_user.id)
            )

        context.user_data.clear()
        context.user_data["mode"] = "diesel_menu"
        return


# === DIESEL RECEIVER CONFIRM FLOW CALLBACKS START ===

    if data.startswith("diesel_receive_detail|"):
        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_photo_id, video_id, created_at, status
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_photo_id, video_id, created_at, status = row

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        media_line = "👁 Кўриш тугмасини босинг — видео очилади." if video_id else "Видео файл йўқ."

        text = (
            "⛽ ДИЗЕЛ МАЪЛУМОТИ\n\n"
            f"Кимдан: {from_car}\n"
            f"Кимга: {to_car}\n"
            f"Сана: {created_text}\n"
            f"Фирма: {firm}\n"
            f"Литр: {liter} л\n"
            f"📍 Спидометр/моточас: {speedometer_photo_id or 'Киритилмаган'}\n"
            f"Изоҳ: {note or ''}\n"
            f"Статус: {diesel_status_display(status)}\n\n"
            f"{media_line}\n\n"
            "Маълумот тўғрими?"
        )

        await query.message.reply_text(
            text,
            reply_markup=diesel_receiver_keyboard(transfer_id)
        )
        return

    if data.startswith("diesel_receive_view|"):
        try:
            await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_photo_id, video_id, created_at, status
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, speedometer_photo_id, video_id, created_at, status = row

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

        text = (
            "⛽ ДИЗЕЛ МАЪЛУМОТИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📍 Спидометр/моточас: {speedometer_photo_id or 'Киритилмаган'}\n"
            f"📝 Изоҳ: {note}\n"
            f"📌 Статус: {diesel_status_display(status)}\n\n"
            "Маълумот тўғрими?"
        )

        await query.message.chat.send_message(text)

        if video_id:
            try:
                await context.bot.send_video_note(
                    chat_id=query.message.chat_id,
                    video_note=video_id
                )
            except Exception:
                await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.chat.send_message(
            "Маълумот тўғрими?",
            reply_markup=diesel_receiver_after_view_keyboard(transfer_id)
        )
        return

    if data.startswith("diesel_receive_accept|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT to_driver_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        to_driver_id = row[0]

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                approved_by_id = %s,
                answered_at = NOW()
            WHERE id = %s
        """, ("Тасдиқланди", int(update.effective_user.id), transfer_id))

        conn.commit()

        await query.message.reply_text("✅ Дизел маълумоти тасдиқланди.\n📌 Статус: Қабул қилинди")
        await notify_diesel_sender_confirmed(context, transfer_id)
        return

    if data.startswith("diesel_receive_reject|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT to_driver_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        to_driver_id = row[0]

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        context.user_data["mode"] = "diesel_receive_reject_note"
        context.user_data["diesel_reject_transfer_id"] = transfer_id

        await query.message.reply_text(
            "❌ Рад этиш сабабини ёзинг.\n\nФақат текст киритинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    
    
    

    if data.startswith("diesel_receiver_rejected_accept|"):
        transfer_id = data.split("|", 1)[1]
    
        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                answered_at = NOW()
            WHERE id = %s
        """, ("Рад этилди", transfer_id))
    
        conn.commit()
    
        await query.message.reply_text("✅ Рад этиш тасдиқланди.")
        return     

    if data.startswith("diesel_receiver_rejected_edit|"):
        transfer_id = data.split("|", 1)[1]
    
        context.user_data["mode"] = "diesel_receive_reject_note"
        context.user_data["diesel_reject_transfer_id"] = transfer_id
    
        await query.message.reply_text(
            "✏️ Янги рад этиш сабабини ёзинг:",
            reply_markup=ReplyKeyboardRemove()
        )
        return          

    if data.startswith("diesel_receiver_rejected_cancel|"):
        transfer_id = data.split("|", 1)[1]
    
        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                receiver_comment = NULL,
                answered_at = NULL
            WHERE id = %s
        """, ("Қабул қилувчи текширувида", transfer_id))
    
        conn.commit()
    
        await query.message.reply_text(
            "❌ Рад этиш бекор қилинди. Маълумот яна текширувда."
        )
        return

    if data.startswith("diesel_rejected_view|"):
        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT
                from_driver_id,
                from_car,
                to_car,
                firm,
                liter,
                note,
                video_id,
                receiver_comment,
                created_at
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_car, firm, liter, note, video_id, receiver_comment, created_at = row

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()
        reject_reason = receiver_comment or "Кўрсатилмаган"

        text = (
            "❌ ДИЗЕЛ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reject_reason}\n"
            "📌 Статус: Рад этилди\n\n"
            "Маълумотни нима қиласиз?"
        )

        if video_id:
            await safe_send_video(
                context.bot,
                query.message.chat_id,
                video_id
            )

        await query.message.reply_text(
            text,
            reply_markup=diesel_rejected_after_view_keyboard(transfer_id)
        )

        return

    if data.startswith("diesel_rejected_cancel|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id = row[0]

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                answered_at = NOW()
            WHERE id = %s
        """, ("Бекор қилинди", transfer_id))

        conn.commit()

        await query.message.reply_text("❌ Рад этилган дизел маълумоти бекор қилинди.")
        return

    if data.startswith("diesel_rejected_resend|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id = row[0]

        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                receiver_comment = NULL,
                answered_at = NULL
            WHERE id = %s
        """, ("Қайта юборилди", transfer_id))

        conn.commit()

        await send_diesel_transfer_to_receiver(context, transfer_id)
        await query.message.reply_text("✅ Маълумот қайта дизел олувчига юборилди.")
        return

    if data.startswith("diesel_receiver_rejected_accept|"):
        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            UPDATE diesel_transfers
            SET status = %s,
                answered_at = NOW()
            WHERE id = %s
        """, ("Рад этилди", transfer_id))

        conn.commit()

        await query.message.reply_text("✅ Рад этиш тасдиқланди.")
        return

async def diesel_rejected_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    transfer_id = query.data.split("|")[1]

    cursor.execute("""
        SELECT
            from_driver_id,
            from_car,
            to_car,
            firm,
            liter,
            note,
            receiver_comment,
            created_at
        FROM diesel_transfers
        WHERE id = %s
    """, (transfer_id,))

    row = cursor.fetchone()

    if not row:
        return

    from_driver_id, from_car, to_car, firm, liter, note, reject_reason, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    text = (
        "❌ ДИЗЕЛ ОЛИШ РАД ЭТИЛДИ\n\n"
        f"🕒 Вақт: {created_text}\n"
        f"🏢 Фирма: {firm}\n"
        f"{diesel_sender_line(from_car, from_driver_id=from_driver_id)}\n"
        f"🚛 Дизел олган: {to_car}\n"
        f"📝 Изоҳ: {note}\n"
        f"⛽ Литр: {liter}\n\n"
        f"❗ Рад этилиш сабаби: {reject_reason}"
    )

    await query.message.reply_text(
        text,
        reply_markup=diesel_rejected_after_view_keyboard(transfer_id)
    )

async def diesel_rejected_resend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("✅ Қайта тасдиқланди")

    transfer_id = query.data.split("|")[1]

    cursor.execute("""
        UPDATE diesel_transfers
        SET status = %s,
            answered_at = NOW()
        WHERE id = %s
    """, ("Берилди", transfer_id))

    conn.commit()

    await query.message.reply_text(
        "✅ ДИЗЕЛ расход қайта тасдиқланди."
    )

async def diesel_rejected_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("❌ Бекор қилинди")

    await query.message.reply_text(
        "❌ Амалиёт бекор қилинди."
    )


    if data == "dieselgive_cancel":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
    
        context.user_data.clear()
    
        await query.message.reply_text(
            "❌ Дизел маълумоти бекор қилинди.",
            reply_markup=diesel_report_keyboard(update.effective_user.id)
        )
        return


    if data == "dieselgive_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
    
        context.user_data["mode"] = "dieselgive_edit_menu"
    
        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=diesel_give_edit_keyboard()
        )
        return


    if data == "dieselgive_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_menu"

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=diesel_give_edit_keyboard()
        )
        return

    if data == "diesel_edit_car":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_firm"

        await query.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=(
                diesel_prihod_firm_stock_keyboard()
                if is_zapravshik_diesel_expense_flow(context)
                else diesel_firm_plain_keyboard()
            )
        )
        return

    if data == "diesel_edit_liter":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_liter"
        
        await query.message.reply_text(
            "⛽ Янги дизел миқдорини киритинг.\n\n"
            "Фақат рақам киритинг.\n"
            "Мисол: 60",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_note":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_note"

        await query.message.reply_text(
            "📝 Янги изоҳни киритинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data == "diesel_edit_video":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "dieselgive_edit_video"
        
        await query.message.reply_text(
            "🎥 Янги думалоқ видео юборинг.\n\n"
            "⏱ Видео 10 сониядан кам бўлмасин.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    

    if data.startswith("dieselgive_car|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = data.split("|", 1)[1]

        context.user_data["dieselgive_to_car"] = car

        if context.user_data.get("mode") == "dieselgive_edit_car":
            context.user_data["mode"] = "dieselgive_confirm"

            await query.message.reply_text(
                diesel_confirm_text(context),
                reply_markup=diesel_give_final_keyboard()
            )
            return

        context.user_data["mode"] = "dieselgive_liter"

        await query.message.reply_text(
            f"🚛 ДИЗЕЛ оладиган техника: {car}\n\n"
            "⛽ Неччи литр ДИЗЕЛ беряпсиз?\n\n"
            "Фақат рақам киритинг.\n"
            "Мисол: 60",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if data.startswith("approve_driver|"):
        driver_ref = data.split("|")[1]

        staff = get_staff_by_id(driver_ref)
        if staff:
            db_id = staff["id"]
            telegram_id = staff["telegram_id"]
        else:
            db_id = None
            telegram_id = driver_ref

        if db_id:
            technadzor_clear_pending_edit_backup(context, db_id)
            cursor.execute("UPDATE drivers SET status = %s WHERE id = %s", ("Тасдиқланди", db_id))
            conn.commit()
        else:
            update_driver_status(telegram_id, "Тасдиқланди")

        try:
            rows = drivers_ws.get_all_values()
            for i, row in enumerate(rows, start=1):
                if len(row) > 0 and str(row[0]) == str(telegram_id):
                    drivers_ws.update_cell(i, 7, "Тасдиқланди")
                    break
        except Exception as e:
            print("APPROVE DRIVER SHEET ERROR:", e)

        try:
            approved_role = staff["work_role"] if staff else get_driver_work_role(telegram_id)

            if approved_role == "mechanic":
                approved_text = "✅ Механик сифатида маълумотларингиз тасдиқланди.\n/start босинг."
            elif approved_role == "zapravshik":
                approved_text = "✅ Заправщик сифатида маълумотларингиз тасдиқланди.\n/start босинг."
            else:
                approved_text = "✅ Ҳайдовчи сифатида маълумотларингиз тасдиқланди.\n/start босинг."

            await context.bot.send_message(
                chat_id=int(telegram_id),
                text=approved_text
            )
        except Exception as e:
            print("APPROVE DRIVER NOTIFY ERROR:", e)

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text("✅ Ходим тасдиқланди")
        return

    if data.startswith("reject_driver|"):
        driver_ref = data.split("|")[1]

        staff = get_staff_by_id(driver_ref)
        if staff:
            db_id = staff["id"]
            telegram_id = staff["telegram_id"]
        else:
            db_id = None
            telegram_id = driver_ref

        if db_id:
            technadzor_clear_pending_edit_backup(context, db_id)
            cursor.execute("UPDATE drivers SET status = %s WHERE id = %s", ("Рад этилди", db_id))
            conn.commit()
        else:
            update_driver_status(telegram_id, "Рад этилди")

        try:
            rows = drivers_ws.get_all_values()
            for i, row in enumerate(rows, start=1):
                if len(row) > 0 and str(row[0]) == str(telegram_id):
                    drivers_ws.update_cell(i, 7, "Рад этилди")
                    break
        except Exception as e:
            print("REJECT DRIVER SHEET ERROR:", e)

        try:
            await context.bot.send_message(
                chat_id=int(telegram_id),
                text="❌ Маълумотларингиз рад этилди.\nАдминистратор билан боғланинг."
            )
        except Exception as e:
            print("REJECT DRIVER NOTIFY ERROR:", e)

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text("❌ Ходим рад этилди")
        return

    if data == "confirm_driver":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        user_id = update.effective_user.id

        if context.user_data.get("driver_work_role") == "zapravshik":
            context.user_data["driver_firm"] = ""
            context.user_data["driver_car"] = ""

        if context.user_data.get("driver_work_role") == "zapravshik":
            context.user_data["driver_firm"] = ""
            context.user_data["driver_car"] = ""

        drivers_ws.append_row([
            user_id,
            context.user_data.get("driver_name", ""),
            context.user_data.get("driver_surname", ""),
            context.user_data.get("phone", ""),
            context.user_data.get("driver_firm", ""),
            context.user_data.get("driver_car", ""),
            "Текширувда",
            now_text(),
            context.user_data.get("driver_work_role", "driver")
        ])
        
        cursor.execute("""
            INSERT INTO drivers (
                telegram_id,
                name,
                surname,
                phone,
                firm,
                car,
                status,
                work_role
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                surname = EXCLUDED.surname,
                phone = EXCLUDED.phone,
                firm = EXCLUDED.firm,
                car = EXCLUDED.car,
                status = EXCLUDED.status,
                work_role = EXCLUDED.work_role
        """, (
            user_id,
            context.user_data.get("driver_name", ""),
            context.user_data.get("driver_surname", ""),
            context.user_data.get("phone", ""),
            context.user_data.get("driver_firm", ""),
            context.user_data.get("driver_car", ""),
            "Текширувда",
            context.user_data.get("driver_work_role", "driver")
        ))
        
        conn.commit()
                        
        for tech_id in get_user_ids_by_role("technadzor"):
            try:
                await context.bot.send_message(
                    chat_id=tech_id,
                    text=("🔔 Уведомления\n👤 Янги ходим\n" + f"{context.user_data.get('driver_surname', '')} {context.user_data.get('driver_name', '')}".strip())
                )
            except Exception:
                pass

        await query.message.reply_text(
            "✅ Рўйхатдан ўтдингиз. Текширувга юборилди."
        )

        context.user_data.clear()
        return


    if data == "edit_driver":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=register_edit_keyboard(context)
        )
        return

    if data.startswith("driver_edit|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        field = data.split("|", 1)[1]

        if field == "name":
            context.user_data["mode"] = "driver_edit_name"
            await query.message.reply_text(
                "🔴 <b>Янги исмни киритинг</b>\n\nМисол: Тешавой",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "surname":
            context.user_data["mode"] = "driver_edit_surname"
            await query.message.reply_text(
                "🔴 <b>Янги фамилияни киритинг</b>\n\nМисол: Алиев",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "phone":
            context.user_data["mode"] = "driver_phone_edit"
            await query.message.reply_text(
                "📞 Янги телефон рақамни юборинг:",
                reply_markup=phone_keyboard()
            )
            return

        if field == "firm":
            if context.user_data.get("driver_work_role") == "zapravshik":
                await query.message.reply_text(
                    "❌ Заправщик учун фирма танлаш керак эмас.",
                    reply_markup=register_edit_keyboard(context)
                )
                return

            if context.user_data.get("driver_work_role") == "mechanic":
                context.user_data["mode"] = "driver_edit_firm_mechanic"
            else:
                context.user_data["mode"] = "driver_edit_firm"

            await query.message.reply_text(
                "🏢 Янги фирмани танланг:",
                reply_markup=firm_keyboard()
            )
            return

        if field == "car":
            if context.user_data.get("driver_work_role") != "driver":
                await query.message.reply_text(
                    "❌ Бу лавозим учун техника таҳрирлаш керак эмас.",
                    reply_markup=register_edit_keyboard(context)
                )
                return

            firm = context.user_data.get("driver_firm")
            context.user_data["mode"] = "driver_edit_car"
            await query.message.reply_text(
                "🚛 Янги техникани танланг:",
                reply_markup=car_buttons_by_firm(firm, only_available_for_driver=True)
            )
            return

    if data.startswith("car_"):

        car = data.replace("car_", "", 1)
        mode = context.user_data.get("mode")
        operation = context.user_data.get("operation")

        # REPAIR SYSTEM — биринчи текширилади.
        # Ремонтга қўшиш техника кнопкалари ҳам car_ билан келади.
        if mode == "choose_car" or operation == "add":

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            context.user_data["operation"] = "add"

            repair_type = context.user_data.get("repair_type")

            await send_last_repairs(query, car, repair_type)

            context.user_data["mode"] = "write_km"

            await query.message.reply_text(
                f"🚛 Техника: {car}\\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\\n"
                f"🔧 Ремонт тури: {repair_type}\\n\\n"
                "🔴 <b>Юрган масофа ёки моточасни киритинг:</b>\\n\\n"
                "Мисол: 125000",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if mode == "remove_car" or operation == "remove":

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            context.user_data["operation"] = "remove"
            context.user_data["mode"] = "write_note_remove"

            await query.message.reply_text(
                f"🚛 Техника: {car}\\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\\n\\n"
                "🔴 <b>Қилинган иш бўйича изоҳ ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        # DRIVER REGISTRATION / EDIT
        if mode in ["driver_car", "driver_edit_car"]:

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["driver_car"] = car
            context.user_data["mode"] = "driver_confirm"

            await show_driver_confirm(query.message, context)
            return

    if data == "confirm_driver":
        user_id = update.effective_user.id

        add_driver_to_sheet(user_id, context.user_data)

        await query.message.reply_text("✅ Рўйхатдан ўтдингиз. Текширувга юборилди.")
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik"]:
        await query.message.reply_text("❌ Сизга рухсат йўқ")
        return


    if query.data == "final_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        keyboard = edit_keyboard_remove() if context.user_data.get("operation") == "remove" else edit_keyboard()

        await query.message.reply_text(
            "🔴 <b>Қайси маълумотни таҳрирлайсиз?</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return

    if query.data.startswith("car|"):
        car = query.data.split("|", 1)[1]
        mode = context.user_data.get("mode")

        if mode == "history_select_car":
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
                
            context.user_data["history_car"] = car
            context.user_data["mode"] = "history_period"

            msg = await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
            remember_inline_message(context, msg)
            return

        if mode == "choose_car" or context.user_data.get("operation") == "add":
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            repair_type = context.user_data.get("repair_type")

            await send_last_repairs(query, car, repair_type)

            context.user_data["mode"] = "write_km"

            await query.message.reply_text(
                f"🚛 Техника: {car}\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\n"
                f"🔧 Ремонт тури: {repair_type}\n\n"
                "🔴 <b>Юрган масофа ёки моточасни киритинг:</b>\n\n"
                "Мисол: 125000",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if mode == "remove_car" or context.user_data.get("operation") == "remove":
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            context.user_data["car"] = car
            context.user_data["mode"] = "write_note_remove"

            await query.message.reply_text(
                f"🚛 Техника: {car}\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\n\n"
                "🔴 <b>Қилинган иш бўйича изоҳ ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

    if query.data.startswith("edit|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        field = query.data.split("|", 1)[1]

        if field == "km":
            context.user_data["mode"] = "edit_km"
            await query.message.reply_text(
                "🔴 <b>Янги КМ/моточасни рақам билан киритинг:</b>\n\nМисол: 125000",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "photo":
            context.user_data["mode"] = "edit_photo"
            await query.message.reply_text(
                "🔴 <b>Янги расмни юборинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "note":
            context.user_data["mode"] = "edit_note"
            await query.message.reply_text(
                "🔴 <b>Янги изоҳни ёзинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        if field == "video":
            context.user_data["mode"] = "edit_video"
            await query.message.reply_text(
                "🔴 <b>Янги думалоқ видео ёки оддий видео файл юборинг:</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

    if query.data.startswith("period|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        period = query.data.split("|", 1)[1]
        if period == "custom":
            context.user_data["mode"] = "history_custom_period"

            await query.message.reply_text(
                "🔴 <b>Қайси давр оралиғини кўрмоқчисиз?</b>\n\n"
                "Мисол:\n"
                "01.01.2026-28.04.2026",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
            
        car = context.user_data.get("history_car")

        if not car:
            await query.message.reply_text("❌ Техника танланмаган.")
            return

        start_date, end_date = get_period_dates(period)

        if not start_date or not end_date:
            await query.message.reply_text("❌ Давр хатолик.")
            return

        await send_history_by_date(query.message, car, start_date, end_date)

        context.user_data["mode"] = None
        return

    if query.data.startswith("check_detail|"):
        try:
            await query.answer()
        except Exception:
            pass

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "confirm_exit_card"
        car = query.data.split("|", 1)[1]
        kirgan_list, chiqqan = get_last_repair_pair(car)

        if not chiqqan:
            await query.message.reply_text("❌ Ремонтдан чиқариш маълумоти топилмади.")
            return

        await query.message.reply_text(
            f"🚛 Техника: {car}\n"
            f"🚜 Тури: {get_car_type(car)}"
        )

        if kirgan_list:
            for kirgan in kirgan_list:
                await query.message.reply_text(
                    "🔴 РЕМОНТГА КИРГАН\n"
                    f"📅 Сана ва вақт: {kirgan[10] if len(kirgan) > 10 else kirgan[1]}\n"
                    f"⏱ КМ/Моточас: {kirgan[3] if len(kirgan) > 3 else ''}\n"
                    f"🔧 Ремонт тури: {kirgan[4] if len(kirgan) > 4 else ''}\n"
                    f"📝 Изоҳ: {clean_note(kirgan[6] if len(kirgan) > 6 else '')}\n"
                    f"👤 Киритган: {kirgan[9] if len(kirgan) > 9 else ''}"
                )

                if len(kirgan) > 8 and kirgan[8]:
                    await safe_send_photo(
                        query.message.get_bot(),
                        query.message.chat_id,
                        kirgan[8]
                    )

                if len(kirgan) > 7 and kirgan[7]:
                    await safe_send_video(
                        query.message.get_bot(),
                        query.message.chat_id,
                        kirgan[7]
                    )

        await query.message.reply_text(
            "🟡 РЕМОНТДАН ЧИҚҚАН\n"
            f"📅 Сана ва вақт: {chiqqan[11] if len(chiqqan) > 11 else chiqqan[1]}\n"
            f"📝 Изоҳ: {clean_note(chiqqan[6] if len(chiqqan) > 6 else '')}\n"
            f"⏳ Кетган вақт: {chiqqan[12] if len(chiqqan) > 12 else ''}\n"
            f"👤 Чиқарган: {chiqqan[9] if len(chiqqan) > 9 else ''}"
        )

        if len(chiqqan) > 7 and chiqqan[7]:
            await safe_send_video(
                query.message.get_bot(),
                query.message.chat_id,
                chiqqan[7]
            )

        msg = await query.message.reply_text(
            "Текширув натижасини танланг:",
            reply_markup=confirm_action_keyboard(car)
        )
        remember_inline_message(context, msg)
        return

    if query.data.startswith("approve|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = query.data.split("|", 1)[1]
        confirmed_by = get_user_name(update)
        current_time = now_text()

        update_car_status(car, "Соз")

        row_id = len(remont_ws.get_all_values())

        remont_ws.append_row([
            row_id,
            current_time,
            car,
            "",
            "Ремонтдан чиқиш тасдиқланди",
            "Соз",
            "Текширувчи тасдиқлади",
            "",
            "",
            confirmed_by,
            "",
            current_time,
            "",
            update.effective_user.id
        ])

        cursor.execute("""
            INSERT INTO repairs (
                car_number,
                repair_type,
                status,
                comment,
                approved_by,
                approved_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW())
        """, (
            car,
            "Ремонтдан чиқиш тасдиқланди",
            "Соз",
            "Текширувчи тасдиқлади",
            confirmed_by
        ))

        conn.commit()

        context.user_data["mode"] = "confirm_exit"
        await query.message.reply_text(
            f"✅ {car} соз деб тасдиқланди.\nҲолат: Соз",
            reply_markup=only_back_keyboard()
        )
        msg = await query.message.reply_text(
            "Текширувда турган техникалар:",
            reply_markup=cars_for_check_by_firm_group()
        )
        remember_inline_message(context, msg)
        return

    if query.data.startswith("reject|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        car = query.data.split("|", 1)[1]
        context.user_data["reject_car"] = car
        context.user_data["mode"] = "reject_reason"

        await query.message.reply_text(
            f"❌ {car} текширувдан ўтмади.\n\n🔴 <b>Сабабини ёзинг:</b>",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    mode = context.user_data.get("mode")

    if mode in ["diesel_prihod_liter", "diesel_prihod_note", "diesel_prihod_video", "diesel_prihod_edit_liter", "diesel_prihod_edit_note", "diesel_prihod_edit_video", "diesel_prihod_db_edit_liter", "diesel_prihod_db_edit_note", "diesel_prihod_db_edit_video", "diesel_prihod_reject_note"]:
        await update.message.reply_text("❌ Бу босқичда расм қабул қилинмайди. Тўғри маълумот киритинг.", reply_markup=back_keyboard())
        return

    if mode in ["diesel_prihod_photo", "diesel_prihod_edit_photo", "diesel_prihod_db_edit_photo"]:
        photo = update.message.photo[-1]
        context.user_data["diesel_prihod_photo_id"] = photo.file_id
        context.user_data["diesel_prihod_telegram_id"] = update.effective_user.id

        if not context.user_data.get("diesel_prihod_time"):
            context.user_data["diesel_prihod_time"] = now_text()

        if mode == "diesel_prihod_db_edit_photo":
            record_id = context.user_data.get("diesel_prihod_editing_db_id")
            if not diesel_prihod_has_staged_for(context, record_id):
                diesel_prihod_row_to_context(record_id, context)

            context.user_data["diesel_prihod_photo_id"] = photo.file_id
            diesel_prihod_mark_staged(context, record_id)

            context.user_data["mode"] = "diesel_prihod_db_card"
            await show_diesel_prihod_staged_card(update.message, context, record_id)
            return

        context.user_data["mode"] = "diesel_prihod_confirm"
        await update.message.reply_text(diesel_prihod_card_text(context), reply_markup=diesel_prihod_confirm_keyboard())
        return

    if mode in ["technadzor_staff_edit_name", "technadzor_staff_edit_surname", "technadzor_staff_edit_phone"]:
        await update.message.reply_text(
            "❌ Бу босқичда расм қабул қилинмайди. Маълумотни текст/телефон рақам кўринишида киритинг.",
            reply_markup=technadzor_staff_back_reply_keyboard()
        )
        return


    mode = context.user_data.get("mode")

    if mode in ["dieselgive_liter", "dieselgive_edit_liter"]:
        await update.message.reply_text(
            "❌ Нотўғри маълумот киритилди.\n\n"
            "⛽ Фақат литр миқдорини рақам билан киритинг.\n"
            "Мисол: 60"
        )
        return

    if mode in ["dieselgive_note", "dieselgive_edit_note"]:
        await update.message.reply_text(
            "❌ Нотўғри маълумот киритилди.\n\n"
            "📝 Бу босқичда фақат изоҳ матн кўринишида қабул қилинади."
        )
        return

    if context.user_data.get("mode") == "gasgive_receiver_reject_note":
        await update.message.reply_text(
            "❌ Бу босқичда фақат текст қабул қилинади.\n\n"
            "📝 Рад этиш сабабини ҳарф ва рақам билан ёзинг."
        )
        return

    if mode in ["dieselgive_speed_photo", "dieselgive_edit_speed_photo"]:
        await update.message.reply_text(
            "❌ Бу босқичда расм қабул қилинмайди.\n\n"
            "📍 Спидометр ёки моточас кўрсаткичини фақат сон билан ёзинг.\n"
            "Мисол: 125000",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode in ["dieselgive_video", "dieselgive_edit_video"]:
        await update.message.reply_text(
            "❌ Бу босқичда фақат думалоқ видео қабул қилинади.\n\n"
            "🎥 10 сониядан кам бўлмаган думалоқ видео юборинг.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode == "fuel_gas_km":
        await update.message.reply_text(
            "❌ Бу босқичда расм қабул қилинмайди.\n\n"
            "🔴 Спидометр кўрсаткичини рақам билан киритинг.\n"
            "Мисол: 15300"
        )
        return

    if mode == "fuel_gas_video":
        await update.message.reply_text(
            "❌ Бу босқичда фақат видео қабул қилинади.\n\n"
            "🎥 Автомобил рақами ва калонка якуний кўрсаткичини видео қилиб юборинг."
        )
        return

    if mode in ["fuel_gas_photo", "fuel_gas_edit_photo"]:
        context.user_data["fuel_photo_id"] = update.message.photo[-1].file_id
        context.user_data["mode"] = "fuel_gas_confirm"

        await update.message.reply_text(
            fuel_gas_confirm_text(context),
            reply_markup=fuel_gas_after_action_keyboard()
        )
        return

    if mode in ["driver_phone", "driver_phone_edit"]:
        await update.message.reply_text(
            "❌ Бу босқичда фақат телефон рақам қабул қилинади.\n\n"
            "Мисол: 998939992020",
            reply_markup=phone_keyboard()
        )
        return

    if mode == "driver_name":
        await update.message.reply_text(
            "❌ Бу босқичда фақат исм матн кўринишида қабул қилинади.\n\n"
            "🔴 <b>Исмингизни киритинг</b>\nМисол: Тешавой",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    if mode == "driver_surname":
        await update.message.reply_text(
            "❌ Бу босқичда фақат фамилия матн кўринишида қабул қилинади.\n\n"
            "🔴 <b>Фамилиянгизни киритинг</b>\nМисол: Алиев",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik"]:
        await deny(update)
        return
       

    if mode in ["write_note_add", "write_note_remove", "edit_note", "reject_reason"]:
        await update.message.reply_text(
            "❌ Сиздан фақат изоҳни матн шаклида ёзишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return
    
    if mode in ["write_km", "edit_km"]:
        await update.message.reply_text(
            "❌ Сиздан фақат 1–8 хонали КМ/моточас рақамини киритишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return
   
    if mode in ["send_video", "edit_video"]:
        await update.message.reply_text(
            "1❌ Думалоқ видео хабар ёки видео файл юборинг!",
            reply_markup=back_keyboard()
        )
        return

    if mode == "edit_photo":
        context.user_data["km_photo_id"] = update.message.photo[-1].file_id
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            f"Текширинг:\n\n"
            f"🚛 Техника: {context.user_data.get('car')}\n"
            f"🔧 Ремонт тури: {context.user_data.get('repair_type') or 'Ремонтдан чиқарилди'}\n"
            f"⏱ КМ/Моточас: {context.user_data.get('km')}\n"
            f"📝 Изоҳ: {context.user_data.get('note')}\n"
            f"🎥 Видео: сақланди ✅\n\n"
            f"Маълумот тўғрими?",
            reply_markup=final_confirm_keyboard()
        )
        return
    if mode != "send_km_photo":
        await update.message.reply_text(
            "1❌ Бу босқичда фақат изоҳ ёзиш талаб қилинади.",
            reply_markup=back_keyboard()
        )
        return

    context.user_data["km_photo_id"] = update.message.photo[-1].file_id
    context.user_data["mode"] = "write_note_add"

    await update.message.reply_text(
        "✅ КМ/моточас расми сақланди.\n\n🔴 <b>Энди носозлик ёки изоҳни ёзинг:</b>",
        parse_mode="HTML",
        reply_markup=back_keyboard()
    )

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    mode = context.user_data.get("mode")

    if mode in ["diesel_prihod_liter", "diesel_prihod_note", "diesel_prihod_photo", "diesel_prihod_edit_liter", "diesel_prihod_edit_note", "diesel_prihod_edit_photo", "diesel_prihod_db_edit_liter", "diesel_prihod_db_edit_note", "diesel_prihod_db_edit_photo", "diesel_prihod_reject_note"]:
        await update.message.reply_text("❌ Бу босқичда видео қабул қилинмайди. Тўғри маълумот киритинг.", reply_markup=back_keyboard())
        return

    if mode in ["diesel_prihod_video", "diesel_prihod_edit_video", "diesel_prihod_db_edit_video"]:
        video_file_id = None
        duration = 0

        if update.message.video_note:
            video_file_id = update.message.video_note.file_id
            duration = update.message.video_note.duration or 0
        elif update.message.video:
            video_file_id = update.message.video.file_id
            duration = update.message.video.duration or 0
        else:
            await update.message.reply_text("❌ Фақат думалоқ видео юборинг.", reply_markup=back_keyboard())
            return

        if duration < 10:
            await update.message.reply_text("❌ Видео 10 секунддан кам бўлмасин. Қайта юборинг.", reply_markup=back_keyboard())
            return

        context.user_data["diesel_prihod_video_id"] = video_file_id

        if mode == "diesel_prihod_edit_video":
            context.user_data["mode"] = "diesel_prihod_confirm"
            await update.message.reply_text(diesel_prihod_card_text(context), reply_markup=diesel_prihod_confirm_keyboard())
            return

        if mode == "diesel_prihod_db_edit_video":
            record_id = context.user_data.get("diesel_prihod_editing_db_id")
            if not diesel_prihod_has_staged_for(context, record_id):
                diesel_prihod_row_to_context(record_id, context)

            context.user_data["diesel_prihod_video_id"] = video_file_id
            diesel_prihod_mark_staged(context, record_id)

            context.user_data["mode"] = "diesel_prihod_db_card"
            await show_diesel_prihod_staged_card(update.message, context, record_id)
            return

        context.user_data["mode"] = "diesel_prihod_photo"
        await update.message.reply_text(
            "🖼 Накладной расмини юборинг.\n\nФақат расм қабул қилинади.",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["technadzor_staff_edit_name", "technadzor_staff_edit_surname", "technadzor_staff_edit_phone"]:
        await update.message.reply_text(
            "❌ Бу босқичда видео қабул қилинмайди. Маълумотни текст/телефон рақам кўринишида киритинг.",
            reply_markup=technadzor_staff_back_reply_keyboard()
        )
        return

    mode = context.user_data.get("mode")

    if mode in ["dieselgive_liter", "dieselgive_edit_liter"]:
        await update.message.reply_text(
            "❌ Нотўғри маълумот киритилди.\n\n"
            "⛽ Фақат литр миқдорини рақам билан киритинг.\n"
            "Мисол: 60",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode in ["dieselgive_note", "dieselgive_edit_note"]:
        await update.message.reply_text(
            "❌ Нотўғри маълумот киритилди.\n\n"
            "📝 Бу босқичда фақат текст изоҳ қабул қилинади.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode in ["dieselgive_speed_photo", "dieselgive_edit_speed_photo"]:
        await update.message.reply_text(
            "❌ Нотўғри маълумот киритилди.\n\n"
            "📍 Спидометр ёки моточас кўрсаткичини фақат сон билан ёзинг.\n"
            "Мисол: 125000",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if mode in ["dieselgive_video", "dieselgive_edit_video"]:
        if not update.message.video_note:
            await update.message.reply_text(
                "❌ Фақат думалоқ видео қабул қилинади.\n\n"
                "🎥 10 сониядан кам бўлмаган думалоқ видео юборинг.",
                reply_markup=back_keyboard()
            )
            return

        if update.message.video_note.duration < 10:
            await update.message.reply_text(
                "❌ Видео 10 сониядан кам.\n\n"
                "🎥 Қайта 10 сониядан кам бўлмаган думалоқ видео юборинг.",
                reply_markup=back_keyboard()
            )
            return

        context.user_data["dieselgive_video_id"] = update.message.video_note.file_id
        context.user_data["dieselgive_created_time"] = now_text()
        context.user_data["mode"] = "dieselgive_confirm"

        await update.message.reply_text(
            diesel_confirm_text(context),
            reply_markup=diesel_give_final_keyboard()
        )
        return

    if context.user_data.get("mode") == "gasgive_receiver_reject_note":
        await update.message.reply_text(
            "❌ Бу босқичда фақат текст қабул қилинади.\n\n"
            "📝 Рад этиш сабабини ҳарф ва рақам билан ёзинг."
        )
        return

    if mode == "gasgive_video":
        if not update.message.video_note:
            await update.message.reply_text(
                "❌ Фақат думалоқ видео қабул қилинади.\n\n"
                "🎥 10 сониядан кам бўлмаган думалоқ видео юборинг."
            )
            return
        
        duration = update.message.video_note.duration
        
        if duration < 10:
            await update.message.reply_text(
                "❌ Видео 10 сониядан кам.\n\n"
                "🎥 Қайта думалоқ видео юборинг."
            )
            return

        context.user_data["gasgive_video_id"] = update.message.video_note.file_id
        context.user_data["mode"] = "gasgive_confirm"

        from_car = context.user_data.get("gasgive_from_car")
        to_car = context.user_data.get("gasgive_to_car")
        note = context.user_data.get("gasgive_note")
        created_time = now_text()

        from_driver = get_driver_by_car(from_car)
        to_driver = get_driver_by_car(to_car)

        context.user_data["gasgive_created_time"] = created_time
        context.user_data["gasgive_from_driver_name"] = short_driver_name(from_driver)
        context.user_data["gasgive_to_driver_name"] = short_driver_name(to_driver)

        sent_msg = await update.message.reply_text(
            gas_confirm_text(context),
            reply_markup=gas_give_confirm_keyboard()
        )

        context.user_data["gasgive_confirm_message_id"] = sent_msg.message_id


        schedule_gas_auto_confirm_task(context, update.effective_user.id)

        return

    if mode == "fuel_gas_km":
        await update.message.reply_text(
            "❌ Бу босқичда видео қабул қилинмайди.\n\n"
            "🔴 Спидометр кўрсаткичини рақам билан киритинг.\n"
            "Мисол: 15300"
        )
        return

    if mode == "fuel_gas_photo":
        await update.message.reply_text(
            "❌ Бу босқичда фақат ведомость расми қабул қилинади.\n\n"
            "📷 Ведомостьга қўл қўйиб, расмга олиб юборинг."
        )
        return

    if mode in ["fuel_gas_video", "fuel_gas_edit_video"]:
        if update.message.video_note:
            video_id = update.message.video_note.file_id
            duration = update.message.video_note.duration
        elif update.message.video:
            video_id = update.message.video.file_id
            duration = update.message.video.duration
        else:
            await update.message.reply_text("❌ Фақат видео юборинг.")
            return

        if duration < 5:
            await update.message.reply_text(
                "❌ Видео 5 сониядан кам бўлмасин.\n\n"
                "🎥 Автомобил рақами ва калонка якуний кўрсаткичини қайта видео қилиб юборинг."
            )
            return

        context.user_data["fuel_video_id"] = video_id

        if mode == "fuel_gas_edit_video":
            context.user_data["mode"] = "fuel_gas_confirm"

            await update.message.reply_text(
                fuel_gas_confirm_text(context),
                reply_markup=fuel_gas_after_action_keyboard()
            )
            return

        context.user_data["mode"] = "fuel_gas_photo"

        await update.message.reply_text(
            "✅ Видео сақланди.\n\n"
            "📷 Энди ведомостьга қўл қўйиб, расмга олиб ташланг."
        )
        return

    if mode in ["driver_phone", "driver_phone_edit"]:
        await update.message.reply_text(
            "❌ Бу босқичда фақат телефон рақам қабул қилинади.\n\n"
            "Мисол: 998939992020",
            reply_markup=phone_keyboard()
        )
        return

    if mode == "driver_name":
        await update.message.reply_text(
            "❌ Бу босқичда фақат исм матн кўринишида қабул қилинади.\n\n"
            "🔴 <b>Исмингизни киритинг</b>\nМисол: Тешавой",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    if mode == "driver_surname":
        await update.message.reply_text(
            "❌ Бу босқичда фақат фамилия матн кўринишида қабул қилинади.\n\n"
            "🔴 <b>Фамилиянгизни киритинг</b>\nМисол: Алиев",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik"]:
        await deny(update)
        return

    if mode in ["write_note_add", "write_note_remove", "edit_note", "reject_reason"]:
        await update.message.reply_text(
            "❌ Сиздан фақат изоҳни матн шаклида ёзишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["send_km_photo", "edit_photo"]:
        await update.message.reply_text(
            "❌ Сиздан фақат одометр ёки моточас расмини юборишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return

    if mode in ["write_km", "edit_km"]:
        await update.message.reply_text(
            "❌ Сиздан фақат 1–8 хонали КМ/моточас рақамини киритишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return

    if update.message.video_note:
        video_id = update.message.video_note.file_id
    elif update.message.video:
        video_id = update.message.video.file_id
    else:
        await update.message.reply_text("❌ Фақат видео юборинг.", reply_markup=back_keyboard())
        return

    if mode == "edit_video":
        context.user_data["video_id"] = video_id
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            f"Текширинг:\n\n"
            f"🚛 Техника: {context.user_data.get('car')}\n"
            f"🔧 Ремонт тури: {context.user_data.get('repair_type') or 'Ремонтдан чиқарилди'}\n"
            f"⏱ КМ/Моточас: {context.user_data.get('km')}\n"
            f"📝 Изоҳ: {context.user_data.get('note')}\n"
            f"🎥 Видео: сақланди ✅\n\n"
            f"Маълумот тўғрими?",
            reply_markup=final_confirm_keyboard()
        )
        return

    if mode not in ["send_video", "edit_video"]:
        await update.message.reply_text(
            "❌ Сиздан фақат думалоқ видео хабар ёки видео файл юборишингизни сўрайман!",
            reply_markup=back_keyboard()
        )
        return

    context.user_data["video_id"] = video_id
    context.user_data["mode"] = "final_check"

    text = (
        f"Текширинг:\n\n"
        f"🚛 Техника: {context.user_data.get('car')}\n"
        f"🔧 Ремонт тури: {context.user_data.get('repair_type') or 'Ремонтдан чиқарилди'}\n"
    )

    if context.user_data.get("operation") == "add":
        text += f"⏱ КМ/Моточас: {context.user_data.get('km')}\n"

    if context.user_data.get("operation") == "remove":
        start = get_last_open_repair_start_time(context.user_data.get("car"))
        end = now_text()
        duration = calculate_duration(start, end)
        text += f"⏳ Ремонт вақти: {duration}\n"

    text += (
        f"📝 Изоҳ: {context.user_data.get('note')}\n"
        f"🎥 Видео: сақланди ✅\n\n"
        f"Маълумот тўғрими?"
    )

    await update.message.reply_text(
        text,
        reply_markup=final_confirm_keyboard()
    )

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await maintenance_guard(update, context):
        return

    mode = context.user_data.get("mode")

    if context.user_data.get("mode") == "gasgive_receiver_reject_note":
        await update.message.reply_text(
            "❌ Бу босқичда фақат текст қабул қилинади.\n\n"
            "📝 Рад этиш сабабини ҳарф ва рақам билан ёзинг."
        )
        return

    if mode not in ["driver_phone", "driver_phone_edit", "technadzor_staff_edit_phone"]:
        return

    contact = update.message.contact

    if mode == "technadzor_staff_edit_phone":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        phone = clean_phone_number(contact.phone_number)

        if not is_valid_phone_number(phone):
            await update.message.reply_text(
                "❌ Телефон рақам нотўғри.\n\n"
                "Фақат 998 билан бошланган 12 та рақам қабул қилинади.\n"
                "Мисол: 998939992020",
                reply_markup=phone_back_keyboard()
            )
            return

        try:
            cursor.execute("UPDATE drivers SET phone = %s WHERE id = %s", (phone, int(driver_id)))
            conn.commit()
        except Exception as e:
            print("TECHNADZOR STAFF PHONE CONTACT UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        context.user_data["mode"] = "technadzor_staff_card"
        await clear_technadzor_staff_inline(context, update.effective_chat.id)
        await update.message.reply_text("✅ Телефон сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
        msg = await update.message.reply_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        remember_inline_message(context, msg)
        return

    context.user_data["inline_disabled_by_start"] = False

    phone = clean_phone_number(contact.phone_number)
    if not is_valid_phone_number(phone):
        await update.message.reply_text(
            "❌ Телефон рақам нотўғри.\n\n"
            "Фақат 998 билан бошланган 12 та рақам қабул қилинади.\n"
            "Мисол: 998939992020",
            reply_markup=phone_keyboard()
        )
        return

    context.user_data["phone"] = phone

    if mode == "driver_phone_edit":
        context.user_data["mode"] = "driver_confirm"
        await show_driver_confirm(update.message, context)
        return

    if context.user_data.get("driver_work_role") == "zapravshik":
        context.user_data["driver_firm"] = ""
        context.user_data["driver_car"] = ""
        context.user_data["mode"] = "driver_confirm"
        await show_driver_confirm(update.message, context)
        return

    context.user_data["mode"] = "driver_firm"

    await update.message.reply_text(
        "🏢 Қайси фирмада ишлайсиз?",
        reply_markup=firm_keyboard()
    )
    return




async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """V20: callback/message ичида хато чиқса, бот тўхтамасин ва Render log'да аниқ кўринсин."""
    logger.error("UPDATE ERROR: %s", update)
    logger.error("EXCEPTION:\n%s", "".join(traceback.format_exception(None, context.error, context.error.__traceback__)))

    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Вақтинча техник хатолик бўлди. Илтимос, менюдан қайта уриниб кўринг."
            )
    except Exception:
        logger.exception("ERROR HANDLER USER NOTIFY FAILED")


app = ApplicationBuilder().token(TOKEN).build()
app.add_error_handler(global_error_handler)

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("clear", clear_chat))
app.add_handler(CommandHandler("id", get_id))
app.add_handler(CallbackQueryHandler(handle_callback))
app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
app.add_handler(MessageHandler(filters.VIDEO_NOTE | filters.VIDEO, handle_video))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))


async def main():
    global MAIN_LOOP

    MAIN_LOOP = asyncio.get_running_loop()

    render_url = os.getenv(
        "WEBHOOK_BASE_URL",
        "https://telegram-bot-r9k8.onrender.com"
    ).rstrip("/")

    webhook_url = f"{render_url}/{TOKEN}"

    await app.initialize()

    await app.bot.set_webhook(
        url=webhook_url,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

    await app.start()

    print("BOT STARTED DEPENDENCY-FREE HYBRID WEBHOOK + EARLY HEALTH SERVER")
    print(f"HEALTH URL: {render_url}/")
    print(f"WEBHOOK URL: {webhook_url}")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())


# === V64 BACK LOGIC ===
# Edit menu back -> card
# Card back -> pending list
