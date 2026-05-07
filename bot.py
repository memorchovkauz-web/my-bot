import os
import json
import re
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

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()


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

conn.commit()

TOKEN = os.getenv("BOT_TOKEN")

USERS = {
    492894595: {"role": "director", "name": "Jahongir Ganiyev"},
    1026372827: {"role": "mechanic", "name": "Пармонов Гиёс"},
    1950294513: {"role": "mechanic", "name": "{Холикулов Шехроз"},
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
sheet = client.open(SHEET_NAME)

remont_ws = sheet.worksheet("REMONT")

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

mashina_ws = sheet.worksheet("MASHINALAR")

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

drivers_ws = sheet.worksheet("DRIVERS")

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
    return datetime.now(ZoneInfo("Asia/Tashkent")).strftime("%d.%m.%Y %H:%M:%S")


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
    now = datetime.now(ZoneInfo("Asia/Tashkent"))

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
    return user["role"] if user else None


def get_user_name(update):
    user = get_user(update)
    return user["name"] if user else "Номаълум"

def get_driver_status(user_id):
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
        [KeyboardButton("🚚 Ҳайдовчилар")],
        [KeyboardButton("📚 История")],
    ], resize_keyboard=True)

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


def gas_cars_by_firm_keyboard(firm, exclude_car=None):
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
                callback_data=f"gasgive_car|{car_number}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Газли техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)


def gas_give_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data="gasgive_confirm"),
            InlineKeyboardButton("✏️ Таҳрирлаш", callback_data="gasgive_edit")
        ]
    ])


def gas_give_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Изоҳ", callback_data="gasgive_edit_note")],
        [InlineKeyboardButton("🎥 Видео", callback_data="gasgive_edit_video")]
    ])


def gas_receiver_confirm_keyboard(transfer_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"gasgive_accept|{transfer_id}"),
            InlineKeyboardButton("❌ Рад этиш", callback_data=f"gasgive_reject|{transfer_id}")
        ]
    ])

def view_media_keyboard(media_key):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁 Кўриш", callback_data=f"view_media|{media_key}")]
    ])


def is_valid_gas_note(text):
    return bool(re.match(r"^[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳ0-9\s]+$", text.strip())) and len(text.strip()) >= 2


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

    if created_at:
        created_at = created_at.strftime("%d.%m.%Y %H:%M:%S")
    
    message_text = (
        "⛽ Сизга ГАЗ бериш маълумоти келди\n\n"
        f"🕒 Вақт: {created_at}\n"
        f"🏢 Фирма: {firm}\n"
        f"🚛 Газ берувчи: {from_car} — {from_driver_name}\n"
        f"🚛 Газ олувчи: {to_car} — {to_driver_name}\n"
        f"📝 Изоҳ: {note}\n\n"
        "Тасдиқлайсизми?"
    )

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text=message_text,
        reply_markup=gas_receiver_confirm_keyboard(transfer_id)
    )

    context.user_data.setdefault("media_store", {})
    context.user_data["media_store"][f"gas_receiver_{transfer_id}"] = {
        "text": message_text,
        "photo_id": "",
        "video_id": video_id
    }

    await context.bot.send_message(
        chat_id=int(to_driver_id),
        text="📎 Расм/видеони кўриш учун пастдаги тугмани босинг:",
        reply_markup=view_media_keyboard(f"gas_receiver_{transfer_id}")
    )

    asyncio.create_task(auto_accept_gas_transfer(context, transfer_id))


async def auto_confirm_gas_transfer(context, user_id):
    await asyncio.sleep(900)  # 15 минут
    
    if context.user_data.get("mode") not in ["gasgive_confirm", "gasgive_edit_menu", "gasgive_edit_note_text", "gasgive_video"]:
        return

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


async def auto_accept_gas_transfer(context, transfer_id):
    await asyncio.sleep(21600)  # 6 соат

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
            COUNT(*) AS total_repairs,
            SUM(CASE WHEN status = 'Соз' THEN 1 ELSE 0 END) AS approved_repairs
        FROM repairs
        GROUP BY car_number
    """)

    stats_rows = cursor.fetchall()

    stats = {}
    for row in stats_rows:
        stats[row[0]] = {
            "total": row[1] or 0,
            "approved": row[2] or 0
        }

    keyboard = []

    for car_number, car_type in cars:
        car_stat = stats.get(car_number, {"total": 0, "approved": 0})

        keyboard.append([
            InlineKeyboardButton(
                f"{car_number} | {car_type} | Р:{car_stat['total']} | Т:{car_stat['approved']}",
                callback_data=f"car|{car_number}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Техника топилмади", callback_data="none")]]

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

async def show_driver_confirm(message, context):
    text = (
        "📋 Маълумотларни текширинг:\n\n"
        f"👤 Исм: {context.user_data.get('driver_name')}\n"
        f"👤 Фамилия: {context.user_data.get('driver_surname')}\n"
        f"📞 Телефон: {context.user_data.get('phone')}\n"
        f"🏢 Фирма: {context.user_data.get('driver_firm')}\n"
        f"🚛 Техника: {context.user_data.get('driver_car')}\n\n"
        "Тасдиқлайсизми?"
    )
    await message.reply_text(
        "✅ Техника танланди.",
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
    one_year_ago = datetime.now(ZoneInfo("Asia/Tashkent")) - timedelta(days=365)

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
            return value
        try:
            return datetime.strptime(str(value).split(".")[0], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    start_dt = start_date.replace(tzinfo=None)
    end_dt = end_date.replace(tzinfo=None)

    cursor.execute("""
        SELECT
            id,
            car_number,
            km,
            repair_type,
            status,
            comment,
            enter_photo,
            enter_video,
            entered_by,
            exited_by,
            approved_by,
            entered_at,
            exited_at,
            approved_at
        FROM repairs
        WHERE LOWER(car_number) = LOWER(%s)
        ORDER BY COALESCE(entered_at, exited_at, approved_at) ASC, id ASC
    """, (car,))

    rows = cursor.fetchall()

    found = False
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

        event_time = entered_at or exited_at or approved_at
        if not event_time:
            continue

        if event_time < start_dt or event_time > end_dt:
            continue

        if status == "Носоз":
            open_repairs.append(row)
            found = True

            await message.reply_text(
                f"🔴 Ремонтга кирган\n\n"
                f"🚘 {car_number}\n"
                f"🔧 Тури: {car_type}\n"
                f"📅 Сана: {event_time.strftime('%d-%m-%Y %H:%M')}\n"
                f"📟 KM/Моточас: {km}\n"
                f"🛠 Ремонт: {repair_type}\n"
                f"📌 Статус: Носоз\n"
                f"💬 Изоҳ: {comment}\n"
                f"👨‍🔧 Киритган: {entered_by}"
            )

            if photo_id or video_id:
                media_key = f"history_enter_{row_id}"

                await message.reply_text(
                    "📎 Расм/видеони кўриш учун:",
                    reply_markup=view_media_keyboard(media_key)
                )

            continue

        if status == "Текширувда":
            pending_exit = row
            continue

        if status == "Соз":
            if not pending_exit:
                continue

            exit_time = to_dt(pending_exit[12]) or to_dt(pending_exit[11])
            exit_comment = pending_exit[5] or ""
            exit_video_id = pending_exit[7] or ""
            exit_person = pending_exit[9] or pending_exit[8] or ""
            approve_person = approved_by or entered_by or ""

            first_start = None
            if open_repairs:
                first_start = to_dt(open_repairs[0][11])

            duration_text = ""
            if first_start and exit_time:
                duration_text = calculate_duration(
                    first_start.strftime("%Y-%m-%d %H:%M:%S"),
                    exit_time.strftime("%Y-%m-%d %H:%M:%S")
                )

            repair_types = ", ".join([r[3] for r in open_repairs if r[3]]) or "Ремонтдан чиқарилди"

            found = True

            await message.reply_text(
                f"🟢 Ремонтдан чиққан\n\n"
                f"🚘 {car_number}\n"
                f"🔧 Тури: {car_type}\n"
                f"📅 Сана: {exit_time.strftime('%d-%m-%Y %H:%M') if exit_time else ''}\n"
                f"⏳ Ремонт учун кетган вақт: {duration_text}\n"
                f"🛠 Ремонт: {repair_types}\n"
                f"📌 Статус: тасдиқланди\n"
                f"💬 Изоҳ: {exit_comment}\n"
                f"👨‍🔧 Чиқарган: {exit_person}\n"
                f"👤 Текширувчи: {approve_person}"
            )

            if exit_video_id:
                media_key = f"history_exit_{row_id}"

                await message.reply_text(
                    "📎 Видеони кўриш учун:",
                    reply_markup=view_media_keyboard(media_key)
                )

            open_repairs = []
            pending_exit = None

    if not found:
        await message.reply_text(
            "Бу вақт оралиғида ремонт историяси топилмади.\n\nБошқа даврни танланг:",
            reply_markup=history_period_keyboard()
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data["history"] = []
    
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
                "🚚 Ҳайдовчи сифатида рўйхатдан ўтинг:",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("🚚 Рўйхатдан ўтиш")]],
                    resize_keyboard=True
                )
            )
            context.user_data["mode"] = "driver_register_start"
            return
            
    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await deny(update)
        return

    if role == "technadzor":
        await update.message.reply_text("🧑‍🔍 Текширувчи менюси:", reply_markup=technadzor_keyboard())
        return

    if role == "mechanic":
        await update.message.reply_text("🔧 Механик менюси\n\nАввал фирмани танланг:", reply_markup=firm_keyboard())
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

    save_repair_to_db(
        car=car,
        km=km,
        repair_type=amal,
        status=status,
        note=note,
        video_id=video_id,
        photo_id=km_photo_id,
        person=added_by,
        start_time=repair_start_time,
        end_time=repair_end_time,
        duration=repair_duration,
        executor_id=executor_id
    )

    if operation == "remove":
        await notify_technadzor_for_check(context, car)

    await message_obj.reply_text(
        f"✅ Маълумот сақланди.\n\n🚛 Техника: {car}\n📌 Ҳолат: {status}",
        reply_markup=technadzor_keyboard() if role == "technadzor" else action_keyboard()
    )

    saved_firm = context.user_data.get("firm")
    context.user_data.clear()

    if role != "technadzor":
        context.user_data["firm"] = saved_firm


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.message.contact:
        return

    text = update.message.text.strip()
    mode = context.user_data.get("mode")

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
            from_driver_id, to_car = row
            await context.bot.send_message(
                chat_id=int(from_driver_id),
                text=(
                    "❌ Газ бериш маълумотингиз рад этилди.\n\n"
                    f"🚛 Техника: {to_car}\n"
                    f"🕒 Вақт: {reject_time}\n"
                    f"📝 Сабаб: {reject_note}"
                )
            )

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
            "✅ МАЪЛУМОТЛАР\n\n"
            f"🕒 Вақт: {created_time}\n"
            f"🚛 ГАЗ берувчи: {from_car} — {context.user_data.get('gasgive_from_driver_name')}\n"
            f"⛽ ГАЗ олувчи: {to_car} — {context.user_data.get('gasgive_to_driver_name')}\n"
            f"📝 Изоҳ: {note}\n\n"
            "Тасдиқлайсизми?",
            reply_markup=gas_give_confirm_keyboard()
        )

        context.user_data["gasgive_confirm_message_id"] = sent_msg.message_id

        video_id = context.user_data.get("gasgive_video_id")
        if video_id:
            await update.message.reply_video_note(video_note=video_id)

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

        if fuel_type.lower() != "газ":
            await update.message.reply_text(
                "❌ Ҳозирча ёқилғи ҳисоботи фақат газли техникалар учун ишлайди."
            )
            return

        context.user_data["mode"] = "fuel_menu"

        await update.message.reply_text(
            "⛽ Ёқилғи ҳисоботи бўлими\n\nАмални танланг:",
            reply_markup=gas_report_keyboard()
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

    if text == "⛽ ГАЗ бериш":
        driver_car = get_driver_car(update.effective_user.id)
        fuel_type = get_car_fuel_type(driver_car)

        if fuel_type.lower() != "газ":
            await update.message.reply_text("ГАЗ бериш фақат газли техника ҳайдовчилари учун.")
            return

        context.user_data["mode"] = "gasgive_firm"
        context.user_data["gasgive_from_car"] = driver_car

        await update.message.reply_text(
            "🏢 Қайси фирмадаги газли техникага ГАЗ беряпсиз?",
            reply_markup=gas_firm_keyboard()
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

    if mode == "driver_edit_firm":
        context.user_data["driver_firm"] = text
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

    if role not in ["director", "mechanic", "technadzor", "slesar"] and not str(context.user_data.get("mode", "")).startswith("driver"):
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
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

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

    if text == "⬅️ Орқага":
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


        if mode == "choose_car":
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

    if role == "technadzor":
        if text == "🔧 Ремонтга қўшиш":
            context.user_data.clear()
            context.user_data["mode"] = "select_firm_for_add"
            await update.message.reply_text("🔴 <b>Фирмани танланг:</b>", parse_mode="HTML", reply_markup=firm_keyboard())
            return

        if text == "☑️ Ремонтдан чиқишини тасдиқлаш":
            context.user_data["mode"] = "confirm_exit"
            await update.message.reply_text("Текширувда турган техникалар:", reply_markup=cars_for_check_by_firm_group())
            return

        if text == "🚚 Ҳайдовчилар":
            await update.message.reply_text(
                "🚚 Ҳайдовчилар бўлими ҳозирча тайёрланмоқда.",
                reply_markup=technadzor_keyboard()
            )
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

    if query.data == "none":
        return

    if query.data == "final_confirm":
        await save_final_data(update, context, query.message)

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.answer("Сақланди ✅")
        return

    if data.startswith("view_media|"):
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        media_key = data.split("|", 1)[1]

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

    if data == "gasgive_confirm":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

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
        conn.commit()

        await query.message.reply_text("✅ Маълумот газ олувчи ҳайдовчига юборилди.")

        await send_gas_transfer_to_receiver(context, transfer_id)

        context.user_data.clear()
        return

    if data == "gasgive_edit":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        context.user_data["mode"] = "gasgive_edit_menu"

        asyncio.create_task(
            auto_confirm_gas_transfer(context, update.effective_user.id)
        )

        await query.message.reply_text(
            "✏️ Қайси маълумотни таҳрирлайсиз?",
            reply_markup=gas_give_edit_keyboard()
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
            RETURNING from_driver_id, to_car
        """, ("Тасдиқланди", transfer_id))

        row = cursor.fetchone()
        conn.commit()

        if row:
            from_driver_id, to_car = row
            await context.bot.send_message(
                chat_id=int(from_driver_id),
                text=f"✅ Газ бериш маълумотингиз тасдиқланди.\n🚛 Техника: {to_car}"
            )

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

    if data.startswith("approve_driver|"):
        driver_id = data.split("|")[1]

        update_driver_status(driver_id, "Тасдиқланди")

        try:
            await context.bot.send_message(
                chat_id=int(driver_id),
                text="✅ Маълумотларингиз тасдиқланди.\nБотдан фойдаланишингиз мумкин."
            )
        except Exception:
            pass

        await query.message.reply_text("✅ Ҳайдовчи тасдиқланди")
        return

    if data.startswith("reject_driver|"):
        driver_id = data.split("|")[1]

        update_driver_status(driver_id, "Рад этилди")

        try:
            await context.bot.send_message(
                chat_id=int(driver_id),
                text="❌ Маълумотларингиз рад этилди.\nАдминистратор билан боғланинг."
            )
        except Exception:
            pass

        await query.message.reply_text("❌ Ҳайдовчи рад этилди")
        return

    if data == "confirm_driver":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
            
        user_id = update.effective_user.id

        drivers_ws.append_row([
            user_id,
            context.user_data.get("driver_name", ""),
            context.user_data.get("driver_surname", ""),
            context.user_data.get("phone", ""),
            context.user_data.get("driver_firm", ""),
            context.user_data.get("driver_car", ""),
            "Текширувда",
            now_text()
        ])
                        
        for tech_id in get_user_ids_by_role("technadzor"):
            try:
                await context.bot.send_message(
                    chat_id=tech_id,
                    text=(
                        "🚚 Янги ҳайдовчи рўйхатдан ўтди:\n\n"
                        f"👤 Исм: {context.user_data.get('driver_name', '')}\n"
                        f"👤 Фамилия: {context.user_data.get('driver_surname', '')}\n"
                        f"📞 Телефон: {context.user_data.get('phone', '')}\n"
                        f"🏢 Фирма: {context.user_data.get('driver_firm', '')}\n"
                        f"🚛 Техника: {context.user_data.get('driver_car', '')}\n\n"
                        "Тасдиқлайсизми?"
                    ),
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("✅ Тасдиқлаш", callback_data=f"approve_driver|{user_id}"),
                            InlineKeyboardButton("❌ Рад этиш", callback_data=f"reject_driver|{user_id}")
                        ]
                    ])
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
            reply_markup=driver_edit_keyboard()
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
            context.user_data["mode"] = "driver_edit_firm"
            await query.message.reply_text(
                "🏢 Янги фирмани танланг:",
                reply_markup=firm_keyboard()
            )
            return

        if field == "car":
            firm = context.user_data.get("driver_firm")
            context.user_data["mode"] = "driver_edit_car"
            await query.message.reply_text(
                "🚛 Янги техникани танланг:",
                reply_markup=car_buttons_by_firm(firm)
            )
            return

    if data.startswith("car_"):

        car = data.replace("car_", "")
    
        # DRIVER REGISTRATION
        if context.user_data.get("mode") in ["driver_car", "driver_edit_car"]:
    
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except:
                pass
    
            context.user_data["driver_car"] = car
            context.user_data["mode"] = "driver_confirm"
    
            await show_driver_confirm(query.message, context)
            return
    
        # REPAIR SYSTEM
        mode = context.user_data.get("mode")
    
        if mode == "choose_car":
    
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except:
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

    if data == "confirm_driver":
        user_id = update.effective_user.id

        add_driver_to_sheet(user_id, context.user_data)

        await query.message.reply_text("✅ Рўйхатдан ўтдингиз. Текширувга юборилди.")
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
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

        if mode == "choose_car":
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

        if mode == "remove_car":
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

    if context.user_data.get("mode") == "gasgive_receiver_reject_note":
        await update.message.reply_text(
            "❌ Бу босқичда фақат текст қабул қилинади.\n\n"
            "📝 Рад этиш сабабини ҳарф ва рақам билан ёзинг."
        )
        return

    if mode == "gasgive_video":
        await update.message.reply_text(
            "❌ Бу босқичда фақат 10 сониядан катта думалоқ видео қабул қилинади."
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

    if mode == "fuel_gas_photo":
        context.user_data["fuel_photo_id"] = update.message.photo[-1].file_id
        context.user_data["mode"] = "fuel_gas_done"

        await update.message.reply_text(
            "✅ Ёқилғи ҳисоботи қабул қилинди.\n\n"
            f"🚛 Техника: {context.user_data.get('fuel_car')}\n"
            f"⛽ Ёқилғи тури: {context.user_data.get('fuel_type')}\n"
            f"📍 Спидометр: {context.user_data.get('fuel_km')} км\n"
            f"🎥 Видео: сақланди ✅\n"
            f"📷 Ведомость расми: сақланди ✅",
            reply_markup=driver_main_keyboard(context.user_data.get("fuel_type", ""))
        )

        context.user_data.clear()
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

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
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
                "🎥 Камида 10 сониялик думалоқ видео юборинг."
            )
            return

        if update.message.video_note.duration < 10:
            await update.message.reply_text(
                "❌ Видео 10 сониядан кам.\n\n"
                "🎥 Камида 10 сониялик думалоқ видео юборинг."
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
            "✅ МАЪЛУМОТЛАР\n\n"
            f"🕒 Вақт: {created_time}\n"
            f"🚛 ГАЗ берувчи: {from_car} — {context.user_data.get('gasgive_from_driver_name')}\n"
            f"⛽ ГАЗ олувчи: {to_car} — {context.user_data.get('gasgive_to_driver_name')}\n"
            f"📝 Изоҳ: {note}\n\n"
            "Тасдиқлайсизми?",
            reply_markup=gas_give_confirm_keyboard()
        )

        context.user_data["gasgive_confirm_message_id"] = sent_msg.message_id

        await update.message.reply_video_note(
            video_note=update.message.video_note.file_id
        )

        asyncio.create_task(
            auto_confirm_gas_transfer(context, update.effective_user.id)
        )

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

    if mode == "fuel_gas_video":
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

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
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

    if mode not in ["driver_phone", "driver_phone_edit"]:
        return

    contact = update.message.contact
    context.user_data["phone"] = contact.phone_number

    if mode == "driver_phone_edit":
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
