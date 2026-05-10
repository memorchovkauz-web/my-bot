import os
import json
import re
import time
import threading
import asyncio
import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, HTTPServer

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

DATABASE_URL = os.getenv("DATABASE_URL")

TASHKENT_TZ = ZoneInfo("Asia/Tashkent")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()
try:
    cursor.execute("SET TIME ZONE 'Asia/Tashkent'")
    conn.commit()
except Exception as e:
    print("DB TIMEZONE SET ERROR:", e)


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
    video_id TEXT,
    status TEXT DEFAULT 'Қабул қилувчи текширувида',
    receiver_comment TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    answered_at TIMESTAMP
)
""")

conn.commit()

TOKEN = os.getenv("BOT_TOKEN")

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
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (telegram_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    surname = EXCLUDED.surname,
                    phone = EXCLUDED.phone,
                    firm = EXCLUDED.firm,
                    car = EXCLUDED.car,
                    status = EXCLUDED.status
            """, (
                int(telegram_id),
                name,
                surname,
                phone,
                firm,
                car,
                status
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
    value = value.strip()
    return value.isalpha() and len(value) >= 2


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
        cursor.execute("""
            SELECT work_role, status
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(update.effective_user.id),))

        row = cursor.fetchone()

        if not row:
            return None

        work_role = row[0] or "driver"
        status = row[1] or ""

        if status != "Тасдиқланди":
            return None

        if work_role in ["mechanic", "zapravshik"]:
            return work_role

        return None

    except Exception as e:
        print("GET ROLE FROM DB ERROR:", e)
        return None


def get_user_name(update):
    user = get_user(update)
    return user["name"] if user else "Номаълум"

def get_driver_status(user_id):
    global conn, cursor

    try:
        cursor.execute("""
            SELECT status
            FROM drivers
            WHERE telegram_id = %s
            LIMIT 1
        """, (int(user_id),))

        row = cursor.fetchone()

        if row:
            return row[0] or ""

        return None

    except Exception as e:
        print("GET DRIVER STATUS ERROR:", e)

        try:
            conn.rollback()
        except:
            pass

        try:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT status
                FROM drivers
                WHERE telegram_id = %s
                LIMIT 1
            """, (int(user_id),))

            row = cursor.fetchone()

            if row:
                return row[0] or ""

        except Exception as e2:
            print("RECONNECT DRIVER STATUS ERROR:", e2)

    return None

def get_driver_car(user_id):
    cursor.execute("""
        SELECT car
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()

    if row:
        return row[0] or ""

    return ""


def get_driver_work_role(user_id):
    cursor.execute("""
        SELECT work_role
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()

    if row:
        return row[0] or "driver"

    return "driver"


def work_role_title(work_role):
    titles = {
        "driver": "Ҳайдовчи",
        "mechanic": "Механик",
        "zapravshik": "Заправщик",
    }

    return titles.get(work_role or "driver", work_role or "Ҳайдовчи")


def get_driver_firm(user_id):
    cursor.execute("""
        SELECT firm
        FROM drivers
        WHERE telegram_id = %s
        LIMIT 1
    """, (int(user_id),))

    row = cursor.fetchone()

    if row:
        return row[0] or ""

    return ""

def update_driver_status(user_id, status):
    cursor.execute("""
        UPDATE drivers
        SET status = %s
        WHERE telegram_id = %s
    """, (status, int(user_id)))

    conn.commit()
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
    return ReplyKeyboardMarkup([
        [KeyboardButton("🔧 Ремонтга қўшиш")],
        [KeyboardButton("☑️ Ремонтдан чиқишини тасдиқлаш")],
        [KeyboardButton("👥 Ходимлар")],
        [KeyboardButton("📚 История")],
    ], resize_keyboard=True)

def pending_registration_count():
    try:
        cursor.execute("SELECT COUNT(*) FROM drivers WHERE status = %s", ("Текширувда",))
        row = cursor.fetchone()
        return int(row[0] or 0)
    except Exception as e:
        print("PENDING REGISTRATION COUNT ERROR:", e)
        return 0


def technadzor_staff_menu_keyboard():
    pending_count = pending_registration_count()

    rows = [
        [KeyboardButton("🚚 Ҳайдовчилар")],
        [KeyboardButton("🔧 Механиклар")],
        [KeyboardButton("⛽ Заправщиклар")],
    ]

    if pending_count > 0:
        rows.append([KeyboardButton(f"📝 Регистрация ({pending_count})")])

    rows.append([KeyboardButton("⬅️ Орқага")])

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def technadzor_staff_firms_reply_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(name)] for name in FIRM_NAMES] + [[KeyboardButton("⬅️ Орқага")]],
        resize_keyboard=True
    )


def technadzor_staff_back_reply_keyboard():
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


async def clear_technadzor_staff_inline(context, chat_id):
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


def get_car_type_by_number(car_number):
    if not car_number:
        return ""

    try:
        cursor.execute("""
            SELECT car_type
            FROM cars
            WHERE LOWER(car_number) = LOWER(%s)
            LIMIT 1
        """, (car_number,))
        row = cursor.fetchone()
        if row:
            return row[0] or ""
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
            WHERE status = 'Текширувда'
            ORDER BY
                CASE
                    WHEN COALESCE(work_role, 'driver') = 'zapravshik' THEN 'Умумий'
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
        group_name = "Умумий" if (work_role or "driver") == "zapravshik" else (firm or "Фирма йўқ")

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
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"approve_driver|{driver_id}"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"tz_staff_edit|{driver_id}"),
        ],
        [InlineKeyboardButton("❌ Рад этиш", callback_data=f"reject_driver|{driver_id}")]
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
    return msg


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


def technadzor_staff_cars_reply_keyboard(firm):
    try:
        cursor.execute("""
            SELECT car_number
            FROM cars
            WHERE firm = %s
            ORDER BY car_number
        """, (firm,))
        cars = [row[0] for row in cursor.fetchall() if row[0]]
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

def driver_main_keyboard(fuel_type=""):
    buttons = [
        [KeyboardButton("⛽ Ёқилғи ҳисоботи")],
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


def diesel_report_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("✅ ДИЗЕЛ олишни тасдиқлаш")],
        [KeyboardButton("⛽ ДИЗЕЛ бериш")],
        [KeyboardButton("⬅️ Орқага")],
    ], resize_keyboard=True)


def register_role_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("🔧 Механик")],
        [KeyboardButton("⛽ Заправщик")],
        [KeyboardButton("🚚 Ҳайдовчи")],
    ], resize_keyboard=True)


def zapravshik_main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("⛽ ДИЗЕЛ бериш")],
        [KeyboardButton("📚 Дизел историяси")],
    ], resize_keyboard=True)


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
            dt.to_car,
            dt.firm,
            dt.created_at,
            COALESCE(c.car_type, '')
        FROM diesel_transfers dt
        LEFT JOIN cars c ON LOWER(c.car_number) = LOWER(dt.from_car)
        WHERE LOWER(dt.to_car) = LOWER(%s)
          AND dt.status = 'Қабул қилувчи текширувида'
        ORDER BY dt.firm ASC, dt.created_at DESC
    """, (driver_car,))

    rows = cursor.fetchall()
    keyboard = []
    current_firm = None

    for transfer_id, from_car, to_car, firm, created_at, car_type in rows:
        firm = firm or "Фирма номаълум"

        if current_firm != firm:
            keyboard.append([InlineKeyboardButton(f"🏢 {firm}", callback_data="none")])
            current_firm = firm

        date_text = created_at.strftime("%d.%m.%y") if created_at else ""

        keyboard.append([
            InlineKeyboardButton(
                f"{from_car} | {car_type} | {date_text}",
                callback_data=f"diesel_receive_detail|{transfer_id}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Тасдиқланмаган дизел маълумоти йўқ", callback_data="none")]]

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


def diesel_confirm_text(context):
    created_time = context.user_data.get("dieselgive_created_time") or now_text()
    context.user_data["dieselgive_created_time"] = created_time

    try:
        shown_time = datetime.strptime(created_time, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M")
    except Exception:
        shown_time = created_time

    return (
        "✅ ДИЗЕЛ БЕРИШ МАЪЛУМОТЛАРИ\n\n"
        f"🚛 Дизел берган техника номери: {context.user_data.get('dieselgive_from_car')}\n"
        f"🚛 Дизел олган техника номери: {context.user_data.get('dieselgive_to_car')}\n"
        f"🕒 Вақт: {shown_time}\n"
        f"⛽ Литр: {context.user_data.get('dieselgive_liter')}\n"
        f"📝 Изоҳ: {context.user_data.get('dieselgive_note')}\n"
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
    cursor.execute("""
        SELECT telegram_id, name, surname
        FROM drivers
        WHERE LOWER(car) = LOWER(%s)
          AND status = 'Тасдиқланди'
        LIMIT 1
    """, (car,))

    return cursor.fetchone()

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

    from_driver = get_driver_by_car(from_car)
    to_driver = get_driver_by_car(to_car)

    from_driver_name = short_driver_name(from_driver)
    to_driver_name = short_driver_name(to_driver)

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
            f"❗ Рад сабаби: {reason}\n\n"
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
    if not receiver:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Газ оладиган техника ҳайдовчиси топилмади."
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

    await context.bot.send_message(
        chat_id=user_id,
        text="✅ Маълумот автоматик тасдиқланди ва газ олувчи ҳайдовчига юборилди."
    )

    await send_gas_transfer_to_receiver(context, transfer_id)

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

    return result

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


def car_buttons_by_firm(firm):
    rows_for_buttons = []

    for row in get_all_cars():
        if len(row) < 7:
            continue

        firm_name = row[0].strip()
        car = row[1].strip()
        turi = row[2].strip()
        holat = row[6].strip()

        if firm_name.lower() == firm.strip().lower():
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

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
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
        return True

    except Exception as e:
        print("UPDATE CAR STATUS ERROR:", e)

        try:
            conn.rollback()
        except:
            pass

        try:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE cars
                SET status = %s
                WHERE LOWER(car_number) = LOWER(%s)
            """, (status, car))

            conn.commit()
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
        [InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_receiver_rejected_edit|{transfer_id}")],
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

    from_driver = get_driver_by_car(from_car)
    to_driver = get_driver_by_car(to_car)

    from_driver_name = short_driver_name(from_driver)
    to_driver_name = short_driver_name(to_driver)

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    message_text = (
        "⛽ Сизга ДИЗЕЛ берилди\n\n"
        f"🕒 Вақт: {created_text}\n"
        f"🏢 Фирма: {firm}\n"
        f"🚛 Дизел берган техника: {from_car} — {from_driver_name}\n"
        f"🚛 Дизел олган техника: {to_car} — {to_driver_name}\n"
        f"📝 Изоҳ: {note}\n"
        f"⛽ Литр: {liter}\n\n"
        "Маълумотни тасдиқлайсизми?"
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text,
        reply_markup=diesel_receiver_keyboard(transfer_id)
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
            "✅ Дизел бериш маълумотингиз тасдиқланди.\n\n"
            f"🚛 Берган техника: {from_car}\n"
            f"🚛 Олган техника: {to_car}\n"
            f"⛽ Литр: {liter}"
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
            f"🚛 Дизел берган техника: {from_car}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reason}\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_sender_keyboard(transfer_id)
    )

async def notify_diesel_receiver_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            to_driver_id,
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

    to_driver_id, from_car, to_car, firm, liter, note, video_id, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=(
            "❌ ДИЗЕЛ ОЛИШ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Дизел берган: {from_car}\n"
            f"🚛 Дизел олган: {to_car}\n"
            f"📝 Изоҳ: {note}\n"
            f"⛽ Литр: {liter}\n\n"
            f"❗ Рад этилиш сабаби: {reason}\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_receiver_keyboard(transfer_id)
    )


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
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"diesel_receiver_rejected_accept|{transfer_id}"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data=f"diesel_receiver_rejected_edit|{transfer_id}")
        ],
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

    from_driver = get_driver_by_car(from_car)
    to_driver = get_driver_by_car(to_car)

    from_driver_name = short_driver_name(from_driver)
    to_driver_name = short_driver_name(to_driver)

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    message_text = (
        "⛽ Сизга ДИЗЕЛ берилди\n\n"
        f"🕒 Вақт: {created_text}\n"
        f"🏢 Фирма: {firm}\n"
        f"🚛 Дизел берган: {from_car} — {from_driver_name}\n"
        f"🚛 Дизел олган: {to_car} — {to_driver_name}\n"
        f"📝 Изоҳ: {note}\n"
        f"⛽ Литр: {liter}\n\n"
        "Маълумотни тасдиқлайсизми?"
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text,
        reply_markup=diesel_receiver_keyboard(transfer_id)
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
            "✅ Дизел бериш маълумотингиз тасдиқланди.\n\\n"
            f"🚛 Берган техника: {from_car}\n"
            f"🚛 Олган техника: {to_car}\n"
            f"⛽ Литр: {liter}"
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
            f"🚛 Дизел берган техника: {from_car}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reason}\n\n"
            "Маълумотни нима қиласиз?"
        ),
        reply_markup=diesel_rejected_sender_keyboard(transfer_id)
    )

async def notify_diesel_receiver_rejected(context, transfer_id, reason):
    cursor.execute("""
        SELECT
            to_driver_id,
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

    to_driver_id, from_car, to_car, firm, liter, note, video_id, created_at = row

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                    "⛽ Заправщик менюси:",
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
                f"🚚 Ҳайдовчи менюси\n\n"
                f"🚛 Техника: {driver_car}\n"
                f"⛽ Ёқилғи тури: {fuel_type}",
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
            
    if role not in ["director", "mechanic", "technadzor", "slesar", "zapravshik"]:
        await deny(update)
        return

    if role == "technadzor":
        await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
        return

    if role == "mechanic":
        await update.message.reply_text("🔧 Механик менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
        return

    if role == "zapravshik":
        await update.message.reply_text("⛽ Заправщик менюси:", reply_markup=zapravshik_main_keyboard())
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
    if not update.message or not update.message.text:
        return

    if update.message.contact:
        return

    text = update.message.text.strip()
    context.user_data["inline_disabled_by_start"] = False
    mode = context.user_data.get("mode")

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
            f"🚚 Ҳайдовчи менюси\n\n"
            f"🚛 Техника: {driver_car}\n"
            f"⛽ Ёқилғи тури: {fuel_type}",
            reply_markup=driver_main_keyboard(fuel_type)
        )
        
        return
        
        fuel_type = get_car_fuel_type(driver_car)

        context.user_data.clear()

        await update.message.reply_text("❌ Дизел олиш рад этилди.")
        await update.message.reply_text(
            f"🚚 Ҳайдовчи менюси\n\n"
            f"🚛 Техника: {driver_car}\n"
            f"⛽ Ёқилғи тури: {fuel_type}",
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

    if text == "⬅️ Орқага" and mode in ["diesel_receive_select", "diesel_receive_reject_note"]:
        context.user_data["mode"] = "diesel_menu"

        await update.message.reply_text(
            "⛽ Дизел ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=diesel_report_keyboard()
        )
        return

    if text == "⬅️ Орқага" and mode in ["fuel_menu", "gasgive_firm", "gasgive_car", "gasgive_note", "gasgive_video", "gasgive_confirm"]:
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if mode == "fuel_menu":
            context.user_data.clear()
            await update.message.reply_text(
                f"🚚 Ҳайдовчи менюси\n\n"
                f"🚛 Техника: {driver_car}\n"
                f"⛽ Ёқилғи тури: {fuel_type}",
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
                reply_markup=diesel_report_keyboard()
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

    if text == "✅ ДИЗЕЛ олишни тасдиқлаш":
        driver_car = get_driver_car(update.effective_user.id)

        if not driver_car:
            await update.message.reply_text("❌ Сизга бириктирилган техника топилмади.")
            return

        context.user_data["mode"] = "diesel_receive_select"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        await update.message.reply_text(
            "✅ Тасдиқланмаган дизел маълумотлари:",
            reply_markup=diesel_pending_confirm_keyboard(driver_car)
        )
        return

    if text == "⛽ ДИЗЕЛ бериш":
        work_role = get_driver_work_role(update.effective_user.id)

        if context.user_data.get("mode") not in ["diesel_menu", None]:
            await update.message.reply_text("❌ Бу амал фақат дизел менюсида ишлайди.")
            return

        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if work_role != "zapravshik" and fuel_type.lower() != "дизел":
            await update.message.reply_text("❌ Дизел бериш фақат дизел техника ҳайдовчилари ёки заправщик учун.")
            return

        context.user_data["mode"] = "dieselgive_firm"
        context.user_data["dieselgive_from_car"] = driver_car if work_role != "zapravshik" else "Заправщик"

        await update.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=diesel_firm_keyboard()
        )
        return


    if text == "⬅️ Орқага" and mode == "diesel_menu":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        context.user_data.clear()

        await update.message.reply_text(
            f"🚚 Ҳайдовчи менюси\n\n"
            f"🚛 Техника: {driver_car}\n"
            f"⛽ Ёқилғи тури: {fuel_type}",
            reply_markup=driver_main_keyboard(fuel_type)
        )
        return

    if text == "⬅️ Орқага" and mode in ["dieselgive_firm", "dieselgive_edit_firm"]:
        context.user_data["mode"] = "diesel_menu"

        await update.message.reply_text(
            "⛽ Дизел ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=diesel_report_keyboard()
        )
        return

    if text == "⬅️ Орқага" and mode in ["dieselgive_car", "dieselgive_edit_car"]:
        if mode == "dieselgive_edit_car":
            context.user_data["mode"] = "dieselgive_edit_firm"
        else:
            context.user_data["mode"] = "dieselgive_firm"

        await update.message.reply_text(
            "🏢 Қайси фирмадаги техникага ДИЗЕЛ беряпсиз?",
            reply_markup=diesel_firm_keyboard()
        )
        return

    if mode in ["dieselgive_firm", "dieselgive_edit_firm"]:
        context.user_data["dieselgive_firm"] = text

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
                text,
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
        phone = text.replace(" ", "").replace("+", "")

        if not phone.isdigit() or len(phone) != 12 or not phone.startswith("998"):
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
            reply_markup=car_buttons_by_firm(text)
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
            reply_markup=car_buttons_by_firm(text)
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

    role = get_role(update)

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
                    f"🚚 Ҳайдовчи менюси\n\n"
                    f"🚛 Техника: {driver_car}\n"
                    f"⛽ Ёқилғи тури: {fuel_type}",
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
        if mode in ["technadzor_staff_edit_name", "technadzor_staff_edit_surname", "technadzor_staff_edit_phone", "technadzor_staff_edit_role", "technadzor_staff_edit_firm", "technadzor_staff_edit_car"]:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            driver_id = context.user_data.get("technadzor_selected_staff_id")
            context.user_data["mode"] = "technadzor_staff_card"
            await update.message.reply_text(
                "✏️ Қайси маълумотни таҳрирлайсиз?",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "✏️ Таҳрирлаш менюси:",
                reply_markup=technadzor_staff_edit_keyboard(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
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
                    reply_markup=firm_keyboard()
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
        if not clean_text or len(clean_text) < 2:
            await update.message.reply_text("❌ Маълумот нотўғри. Қайта киритинг.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        column = "name" if mode == "technadzor_staff_edit_name" else "surname"
        try:
            cursor.execute(f"UPDATE drivers SET {column} = %s WHERE id = %s", (clean_text, int(driver_id)))
            conn.commit()
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
        return


    if mode == "technadzor_staff_edit_phone":
        driver_id = context.user_data.get("technadzor_selected_staff_id")
        phone = text.replace(" ", "").replace("+", "")

        if not phone.isdigit() or len(phone) != 12 or not phone.startswith("998"):
            await update.message.reply_text(
                "❌ Телефон рақам нотўғри.\n\nМисол: 998939992020",
                reply_markup=phone_keyboard()
            )
            return

        try:
            cursor.execute("UPDATE drivers SET phone = %s WHERE id = %s", (phone, int(driver_id)))
            conn.commit()
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

        try:
            if new_role == "zapravshik":
                cursor.execute(
                    "UPDATE drivers SET work_role = %s, firm = NULL, car = NULL WHERE id = %s",
                    (new_role, int(driver_id))
                )
            elif new_role == "mechanic":
                cursor.execute(
                    "UPDATE drivers SET work_role = %s, car = NULL WHERE id = %s",
                    (new_role, int(driver_id))
                )
            else:
                cursor.execute(
                    "UPDATE drivers SET work_role = %s, firm = NULL, car = NULL WHERE id = %s",
                    (new_role, int(driver_id))
                )
            conn.commit()
        except Exception as e:
            print("TECHNADZOR STAFF ROLE UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        if new_role == "zapravshik":
            context.user_data.pop("technadzor_staff_pending_role", None)
            context.user_data["mode"] = "technadzor_staff_card"
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            await update.message.reply_text("✅ Лавозим заправщик қилиб сақланди.", reply_markup=technadzor_staff_back_reply_keyboard())
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            return

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
        except Exception as e:
            print("TECHNADZOR STAFF FIRM UPDATE ERROR:", e)
            await update.message.reply_text("❌ Сақлашда хато.", reply_markup=technadzor_staff_back_reply_keyboard())
            return

        if work_role == "driver":
            context.user_data["mode"] = "technadzor_staff_edit_car"
            await update.message.reply_text(
                "✅ Фирма ўзгарди. Энди шу фирма техникани танланг:",
                reply_markup=technadzor_staff_cars_reply_keyboard(text)
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
            await update.message.reply_text("❌ Техникани пастки менюдан танланг.", reply_markup=technadzor_staff_cars_reply_keyboard(firm))
            return

        car_number = row[0]
        try:
            cursor.execute("UPDATE drivers SET work_role = %s, car = %s WHERE id = %s", ("driver", car_number, int(driver_id)))
            conn.commit()
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

        if driver_id and technadzor_rollback_pending_edit(context, driver_id):
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            await update.message.reply_text("↩️ Таҳрирлаш бекор қилинди. Эски маълумотлар тикланди.", reply_markup=technadzor_staff_back_reply_keyboard())
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            return

        if driver_id:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            msg = await update.message.reply_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
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

        if text == "👥 Ходимлар":
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
                    reply_markup=technadzor_staff_firms_reply_keyboard()
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
                return

            if text.startswith("📝 Регистрация"):
                await clear_technadzor_staff_inline(context, update.effective_chat.id)
                context.user_data["mode"] = "technadzor_registration_list"
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
                return

        if mode == "technadzor_staff_drivers_firm" and text in FIRM_NAMES:
            await clear_technadzor_staff_inline(context, update.effective_chat.id)
            context.user_data["mode"] = "technadzor_staff_drivers_list"
            context.user_data["technadzor_staff_type"] = "drivers"
            context.user_data["technadzor_staff_firm"] = text
            context.user_data["technadzor_staff_action_stack"] = []
            await update.message.reply_text(
                f"🚚 Ҳайдовчилар\n🏢 {text}",
                reply_markup=technadzor_staff_back_reply_keyboard()
            )
            msg = await update.message.reply_text(
                "🚚 Ҳайдовчилар рўйхати:",
                reply_markup=technadzor_staff_list_inline_keyboard("drivers", text)
            )
            context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
            return

        if text == "🔧 Ремонтга қўшиш":
            context.user_data.clear()
            context.user_data["mode"] = "select_firm_for_add"
            await update.message.reply_text("🔴 <b>Фирмани танланг:</b>", parse_mode="HTML", reply_markup=firm_back_keyboard())
            return

        if text == "☑️ Ремонтдан чиқишини тасдиқлаш":
            context.user_data["mode"] = "confirm_exit"
            await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
            return

        if text == "📚 История":
            context.user_data["mode"] = "history_select_firm"
            await update.message.reply_text("Қайси фирма техникасини кўрмоқчисиз?", reply_markup=firm_keyboard())
            return

    if mode == "history_select_firm" and text in FIRM_NAMES:
        context.user_data["firm"] = text
        context.user_data["mode"] = "history_select_car"

        await update.message.reply_text("⬅️ Орқага қайтиш учун пастдаги тугмани босинг.", reply_markup=back_keyboard())
        await update.message.reply_text("Техникани танланг:", reply_markup=history_car_buttons_by_firm(text))
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
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "none":
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
                reply_markup=car_buttons_by_firm(firm)
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

            await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
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
            work_role = context.user_data.get("driver_work_role", "driver")
            employee_text = (
                "👤 Янги ходим рўйхатдан ўтди:\n\n"
                f"👤 Лавозим: {work_role_title(work_role)}\n"
                f"👤 Исм: {context.user_data.get('driver_name', '')}\n"
                f"👤 Фамилия: {context.user_data.get('driver_surname', '')}\n"
                f"📞 Телефон: {context.user_data.get('phone', '')}\n"
            )

            if work_role == "mechanic":
                employee_text += f"🏢 Фирма: {context.user_data.get('driver_firm', '')}\n"

            if work_role == "driver":
                employee_text += (
                    f"🏢 Фирма: {context.user_data.get('driver_firm', '')}\n"
                    f"🚛 Техника: {context.user_data.get('driver_car', '')}\n"
                )

            employee_text += "\nТасдиқлайсизми?"

            for tech_id in get_user_ids_by_role("technadzor"):
                try:
                    await context.bot.send_message(
                        chat_id=tech_id,
                        text=employee_text,
                        reply_markup=InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"approve_driver|{user_id}"),
                                InlineKeyboardButton("❌ Рад этиш", callback_data=f"reject_driver|{user_id}")
                            ]
                        ])
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

    if data.startswith("tz_reg"):
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
        await query.edit_message_text(
            "🚚 Ҳайдовчилар\n\nФирмани танланг:",
            reply_markup=technadzor_staff_firms_keyboard("drivers")
        )
        return

    if data == "tz_staff|mechanics":
        context.user_data["technadzor_staff_type"] = "mechanics"
        context.user_data.pop("technadzor_staff_firm", None)
        await query.edit_message_text(
            technadzor_staff_list_text("mechanics"),
            reply_markup=technadzor_staff_list_inline_keyboard("mechanics")
        )
        return

    if data == "tz_staff|zapravshik":
        context.user_data["technadzor_staff_type"] = "zapravshik"
        context.user_data.pop("technadzor_staff_firm", None)
        await query.edit_message_text(
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

        await query.edit_message_text(
            technadzor_staff_list_text(staff_type, firm),
            reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
        )
        return

    if data.startswith("tz_reg_view|"):
        driver_id = data.split("|", 1)[1]
        context.user_data["mode"] = "technadzor_staff_card"
        context.user_data["technadzor_selected_staff_id"] = driver_id
        await query.edit_message_text(
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
        context.user_data["technadzor_selected_staff_id"] = driver_id
        await query.edit_message_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        return

    if data.startswith("tz_staff_edit|"):
        driver_id = data.split("|", 1)[1]
        technadzor_save_pending_edit_backup(context, driver_id)

        context.user_data.setdefault("technadzor_staff_action_stack", []).append({
            "screen": "card",
            "driver_id": driver_id,
        })
        context.user_data["technadzor_selected_staff_id"] = driver_id
        await query.edit_message_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=technadzor_staff_edit_keyboard(driver_id)
        )
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
            await query.message.reply_text("📞 Янги телефон рақамни юборинг:", reply_markup=phone_keyboard())
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
            await query.message.reply_text("🚛 Янги техникани танланг:", reply_markup=technadzor_staff_cars_reply_keyboard(firm))
            return

    if data.startswith("tz_staff_status|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            await query.answer("Маълумот нотўғри.", show_alert=True)
            return

        driver_id = parts[1]
        new_label = parts[2]
        new_status = staff_status_db(new_label)
        try:
            cursor.execute("UPDATE drivers SET status = %s WHERE id = %s", (new_status, int(driver_id)))
            conn.commit()
        except Exception as e:
            print("TECHNADZOR STAFF STATUS UPDATE ERROR:", e)
            await query.answer("❌ Сақлашда хато.", show_alert=True)
            return

        await query.edit_message_text(
            technadzor_staff_card_text(driver_id),
            reply_markup=technadzor_staff_card_reply_markup(driver_id)
        )
        return

    if data == "tz_staff_action_back":
        driver_id = context.user_data.get("technadzor_selected_staff_id")

        if driver_id and technadzor_rollback_pending_edit(context, driver_id):
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            await query.message.reply_text("↩️ Таҳрирлаш бекор қилинди. Эски маълумотлар тикланди.")
            await query.edit_message_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            return

        if driver_id:
            staff = get_staff_by_id(driver_id)
            if staff and staff.get("status") == "Текширувда":
                await query.edit_message_text(
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
            await query.edit_message_text(
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            return

        if last.get("screen") == "list":
            staff_type = last.get("staff_type", "drivers")
            firm = last.get("firm")
            await query.edit_message_text(
                technadzor_staff_list_text(staff_type, firm),
                reply_markup=technadzor_staff_list_inline_keyboard(staff_type, firm)
            )
            return

        if last.get("screen") == "card":
            driver_id = last.get("driver_id")
            await query.edit_message_text(
                technadzor_staff_card_text(driver_id),
                reply_markup=technadzor_staff_card_reply_markup(driver_id)
            )
            return

        if last.get("screen") == "edit_menu":
            driver_id = last.get("driver_id")
            staff = get_staff_by_id(driver_id)
            if staff and staff.get("status") == "Текширувда":
                await query.edit_message_text(
                    technadzor_staff_card_text(driver_id),
                    reply_markup=technadzor_pending_registration_card_keyboard(driver_id)
                )
                return

            await query.edit_message_text(
                "✏️ Қайси маълумотни таҳрирлайсиз?",
                reply_markup=technadzor_staff_edit_keyboard(driver_id)
            )
            return

    if data.startswith("tz_staff_back|"):
        back_to = data.split("|", 1)[1]

        if back_to == "staff_menu":
            await query.edit_message_text(
                "👥 Ходимлар менюси",
                reply_markup=technadzor_staff_menu_inline_keyboard()
            )
            return

        if back_to == "drivers_firms":
            await query.edit_message_text(
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
        if not receiver:
            await query.message.reply_text("❌ Газ оладиган техника ҳайдовчиси топилмади.")
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
            f"❗ Рад сабаби: {reject_reason}\n\n"
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
            UPDATE gas_transfers
            SET status = %s,
                receiver_comment = NULL,
                answered_at = NULL
            WHERE id = %s
        """, ("Қабул қилувчи текширувида", transfer_id))

        conn.commit()

        await send_gas_transfer_to_receiver(context, transfer_id)
        await query.message.reply_text("✅ Маълумот қайта газ олувчига юборилди.", reply_markup=gas_report_keyboard())
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
        """, ("Тасдиқланди", transfer_id))

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
            reply_markup=diesel_report_keyboard()
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
            reply_markup=diesel_firm_keyboard()
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

        if video_id:
            await safe_send_video(context.bot, query.message.chat_id, video_id)

        await query.message.reply_text(
            "❌ ДИЗЕЛ МАЪЛУМОТИ РАД ЭТИЛДИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Дизел берган: {from_car}\n"
            f"🚛 Дизел олган: {to_car}\n"
            f"📝 Изоҳ: {note}\n"
            f"⛽ Литр: {liter}\n\n"
            f"❗ Рад этилиш сабаби: {reject_reason}\n\n"
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
                video_id
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))
    
        row = cursor.fetchone()
    
        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return
    
        from_driver_id, from_car, to_car, firm, liter, note, video_id = row
    
        if int(update.effective_user.id) != int(from_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return
    
        context.user_data["diesel_edit_rejected_id"] = transfer_id
        context.user_data["dieselgive_from_car"] = from_car or ""
        context.user_data["dieselgive_to_car"] = to_car or ""
        context.user_data["dieselgive_firm"] = firm or ""
        context.user_data["dieselgive_liter"] = liter or ""
        context.user_data["dieselgive_note"] = note or ""
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

        to_driver_id, from_car, to_car, firm, liter, note, video_id, receiver_comment, created_at = row

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
            f"🚛 Дизел берган: {from_car}\n"
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

            await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
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
        note = context.user_data.get("dieselgive_note")
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

            if not receiver:
                await query.message.reply_text("❌ Дизел оладиган техника ҳайдовчиси топилмади.")
                return

            to_driver_id = receiver[0]

            cursor.execute("""
                UPDATE diesel_transfers
                SET from_car = %s,
                    to_driver_id = %s,
                    to_car = %s,
                    firm = %s,
                    liter = %s,
                    note = %s,
                    video_id = %s,
                    status = %s,
                    receiver_comment = NULL,
                    answered_at = NULL
                WHERE id = %s
            """, (
                from_car,
                to_driver_id,
                to_car,
                firm,
                liter,
                note,
                video_id,
                "Қабул қилувчи текширувида",
                rejected_transfer_id
            ))

            conn.commit()

            try:
                await send_diesel_transfer_to_receiver(context, rejected_transfer_id)
            except Exception as e:
                print("REJECTED DIESEL RESEND ERROR:", e)

            await query.message.reply_text(
                "✅ Таҳрирланган дизел маълумоти қайта олувчи ҳайдовчига юборилди.",
                reply_markup=diesel_report_keyboard()
            )

            context.user_data.clear()
            context.user_data["mode"] = "diesel_menu"
            return

        receiver = get_driver_by_car(to_car)

        if not receiver:
            await query.message.reply_text("❌ Дизел оладиган техника ҳайдовчиси топилмади.")
            return

        to_driver_id = receiver[0]

        cursor.execute("""
            INSERT INTO diesel_transfers (
                from_driver_id,
                from_car,
                to_driver_id,
                to_car,
                firm,
                liter,
                note,
                video_id,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            user_id,
            from_car,
            to_driver_id,
            to_car,
            firm,
            liter,
            note,
            video_id,
            "Қабул қилувчи текширувида"
        ))

        transfer_id = cursor.fetchone()[0]
        conn.commit()

        await send_diesel_transfer_to_receiver(context, transfer_id)

        await query.message.reply_text(
            "✅ Дизел бериш маълумоти сақланди ва олувчи ҳайдовчига юборилди.",
            reply_markup=diesel_report_keyboard()
        )

        context.user_data.clear()
        context.user_data["mode"] = "diesel_menu"
        return


# === DIESEL RECEIVER CONFIRM FLOW CALLBACKS START ===

    if data.startswith("diesel_receive_view|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        transfer_id = data.split("|", 1)[1]

        cursor.execute("""
            SELECT from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, video_id, created_at
            FROM diesel_transfers
            WHERE id = %s
        """, (transfer_id,))

        row = cursor.fetchone()

        if not row:
            await query.message.reply_text("❌ Маълумот топилмади.")
            return

        from_driver_id, from_car, to_driver_id, to_car, firm, liter, note, video_id, created_at = row

        if int(update.effective_user.id) != int(to_driver_id):
            await query.message.reply_text("❌ Бу маълумот сиз учун эмас.")
            return

        created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

        text = (
            "⛽ ДИЗЕЛ МАЪЛУМОТИ\n\n"
            f"🕒 Вақт: {created_text}\n"
            f"🏢 Фирма: {firm}\n"
            f"🚛 Дизел берган техника: {from_car}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
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
                answered_at = NOW()
            WHERE id = %s
        """, ("Тасдиқланди", transfer_id))

        conn.commit()

        await query.message.reply_text("✅ Дизел маълумоти тасдиқланди.")
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
            f"🚛 Дизел берган техника: {from_car}\n"
            f"🚛 Дизел олган техника: {to_car}\n"
            f"⛽ Литр: {liter}\n"
            f"📝 Изоҳ: {note}\n\n"
            f"❗ Рад сабаби: {reject_reason}\n\n"
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

    from_car, to_car, firm, liter, note, reject_reason, created_at = row

    created_text = created_at.strftime("%d.%m.%Y %H:%M") if created_at else now_text()

    text = (
        "❌ ДИЗЕЛ ОЛИШ РАД ЭТИЛДИ\n\n"
        f"🕒 Вақт: {created_text}\n"
        f"🏢 Фирма: {firm}\n"
        f"🚛 Дизел берган: {from_car}\n"
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
        "✅ Дизел бериш қайта тасдиқланди."
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
            reply_markup=diesel_report_keyboard()
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
            reply_markup=diesel_firm_keyboard()
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
                    text="👤 Янги ходим рўйхатдан ўтди.\n\nТекшириш учун: 👥 Ходимлар → Регистрация менюсига киринг."
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
                reply_markup=car_buttons_by_firm(firm)
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

            await query.message.reply_text(
                f"🚛 Техника: {car}\n\nҚайси давр бўйича история керак?",
                reply_markup=history_period_keyboard()
            )
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
        car = query.data.split("|", 1)[1]
        kirgan_list, chiqqan = get_last_repair_pair(car)

        if not chiqqan:
            await query.message.reply_text("❌ Ремонтдан чиқариш маълумоти топилмади.")
            return

        await query.message.reply_text(
            f"🚛 Техника: {car}\n🚜 Тури: {get_car_type(car)}"
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
                    await safe_send_photo(query.message.get_bot(), query.message.chat_id, kirgan[8])

                if len(kirgan) > 7 and kirgan[7]:
                    await safe_send_video(query.message.get_bot(), query.message.chat_id, kirgan[7])

        await query.message.reply_text(
            "🟡 РЕМОНТДАН ЧИҚҚАН\n"
            f"📅 Сана ва вақт: {chiqqan[11] if len(chiqqan) > 11 else chiqqan[1]}\n"
            f"📝 Изоҳ: {clean_note(chiqqan[6] if len(chiqqan) > 6 else '')}\n"
            f"⏳ Кетган вақт: {chiqqan[12] if len(chiqqan) > 12 else ''}\n"
            f"👤 Чиқарган: {chiqqan[9] if len(chiqqan) > 9 else ''}"
        )

        if len(chiqqan) > 7 and chiqqan[7]:
            await safe_send_video(query.message.get_bot(), query.message.chat_id, chiqqan[7])

        await query.message.reply_text(
            "Текширув натижасини танланг:",
            reply_markup=confirm_action_keyboard(car)
        )
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

        await query.message.reply_text(
            f"✅ {car} соз деб тасдиқланди.\nҲолат: Соз",
            reply_markup=cars_for_check_by_firm_group()
        )
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
        phone = (contact.phone_number or "").replace(" ", "").replace("+", "")
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
        msg = await update.message.reply_text(technadzor_staff_card_text(driver_id), reply_markup=technadzor_staff_card_keyboard(driver_id))
        context.user_data["technadzor_staff_inline_message_id"] = msg.message_id
        return

    context.user_data["inline_disabled_by_start"] = False
    context.user_data["phone"] = contact.phone_number

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


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()


def run_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("clear", clear_chat))
app.add_handler(CommandHandler("id", get_id))
app.add_handler(CallbackQueryHandler(handle_callback))
app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
app.add_handler(MessageHandler(filters.VIDEO_NOTE | filters.VIDEO, handle_video))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))


print("BOT STARTED WEBHOOK")

app.run_webhook(
    listen="0.0.0.0",
    port=int(os.environ.get("PORT", 10000)),
    url_path=TOKEN,
    webhook_url=f"https://telegram-bot-r9k8.onrender.com/{TOKEN}",
    drop_pending_updates=True,
    allowed_updates=Update.ALL_TYPES,
)
