import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import os
import json

TOKEN = os.getenv("BOT_TOKEN")

USERS = {
    492894595: {"role": "director", "name": "Jahongir Ganiyev"},
    1950294513: {"role": "mechanic", "name": "Механик исми"},
    492894594: {"role": "technadzor", "name": "Текширувчи исми"},
    444444444: {"role": "slesar", "name": "Слесарь исми"},
}

SHEET_NAME = "Avtobaza Remont Baza"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
from google.oauth2.service_account import Credentials

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME)

remont_ws = sheet.worksheet("REMONT")
mashina_ws = sheet.worksheet("MASHINALAR")

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
    return datetime.now(ZoneInfo("Asia/Tashkent")).strftime("%Y-%m-%d %H:%M:%S")


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
    now = datetime.now()

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


def history_period_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Охирги 10 кун", callback_data="period|10")],
        [InlineKeyboardButton("📆 Охирги 30 кун", callback_data="period|30")],
        [InlineKeyboardButton("🗓 Шу ой", callback_data="period|this_month")],
        [InlineKeyboardButton("📌 Ўтган ой", callback_data="period|last_month")],
        [InlineKeyboardButton("📚 Охирги 1 йил", callback_data="period|year")],
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
        [InlineKeyboardButton("🎥 Видео", callback_data="edit|video")]
    ])

def get_all_cars():
    return mashina_ws.get_all_values()[1:]


def get_car_type(car):
    for row in get_all_cars():
        if len(row) > 2 and row[1].strip().lower() == car.strip().lower():
            return row[2].strip()
    return ""


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
                callback_data=f"car|{car}"
            )
        ])

    if not keyboard:
        keyboard = [[InlineKeyboardButton("❌ Техника топилмади", callback_data="none")]]

    return InlineKeyboardMarkup(keyboard)


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
                    keyboard.append([
                        InlineKeyboardButton(f"🏢 {firm}", callback_data="none")
                    ])
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


def update_car_status(car, status):
    rows = mashina_ws.get_all_values()

    for i, row in enumerate(rows, start=1):
        if len(row) > 1 and row[1].strip().lower() == car.strip().lower():
            mashina_ws.update_cell(i, 7, status)
            return True

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

    for row in reversed(rows):
        if len(row) < 13:
            continue

        if row[2].strip().lower() != car.strip().lower():
            continue

        status = row[5].strip()

        if status == "Текширувда" and chiqqan is None:
            chiqqan = row

        elif status == "Носоз" and kirgan is None:
            kirgan = row

        if kirgan and chiqqan:
            break

    return kirgan, chiqqan


async def send_last_repairs(query, car, repair_type):
    rows = remont_ws.get_all_values()[1:]
    result = []
    one_year_ago = datetime.now() - timedelta(days=365)

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

        if sana < one_year_ago:
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
            await query.message.reply_video_note(item["video_id"])


async def send_history_by_date(message, car, start_date, end_date):
    rows = remont_ws.get_all_values()[1:]
    car_type = get_car_type(car)
    text_result = []

    for row in rows:
        if len(row) < 13:
            continue

        if row[2].strip().lower() != car.strip().lower():
            continue

        sana_text = row[1].strip()

        try:
            sana = datetime.strptime(sana_text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        if not (start_date <= sana <= end_date):
            continue

        km = row[3] if len(row) > 3 else ""
        repair_type = row[4] if len(row) > 4 else ""
        status = row[5] if len(row) > 5 else ""
        note = clean_note(row[6] if len(row) > 6 else "")
        video_id = row[7] if len(row) > 7 else ""
        person = row[9] if len(row) > 9 else ""
        start_time = row[10] if len(row) > 10 else ""
        end_time = row[11] if len(row) > 11 else ""
        duration = row[12] if len(row) > 12 else ""

        if status == "Носоз":
            block = (
                f"🚛 Техника: {car}\n"
                f"🚜 Тури: {car_type}\n\n"
                f"🔴 РЕМОНТГА КИРДИ\n"
                f"📅 Сана ва вақт: {start_time or sana_text}\n"
                f"⏱ КМ/Моточас: {km}\n"
                f"🔧 Ремонт тури: {repair_type}\n"
                f"📝 Изоҳ: {note}\n"
                f"🎥 Видео: сақланган ✅\n"
                f"👤 Ремонтга киритган шахс: {person}"
            )

        elif status == "Текширувда":
            block = (
                f"🚛 Техника: {car}\n"
                f"🚜 Тури: {car_type}\n\n"
                f"🟡 РЕМОНТДАН ЧИҚДИ\n"
                f"📅 Сана ва вақт: {end_time or sana_text}\n"
                f"📝 Изоҳ: {note}\n"
                f"🎥 Видео: сақланган ✅\n"
                f"⏳ Ремонтга кетган вақт: {duration}\n"
                f"👤 Ремонтдан чиқарган шахс: {person}"
            )

        elif status == "Соз" and "тасдиқ" in repair_type.lower():
            block = (
                f"🚛 Техника: {car}\n"
                f"🚜 Тури: {car_type}\n\n"
                f"✅ ТАСДИҚЛАНДИ\n"
                f"📅 Сана ва вақт: {sana_text}\n"
                f"👤 Технодзор: {person}"
            )

        else:
            block = (
                f"🚛 Техника: {car}\n"
                f"🚜 Тури: {car_type}\n\n"
                f"📌 Ҳолат: {status}\n"
                f"📅 Сана ва вақт: {sana_text}\n"
                f"🔧 Амал: {repair_type}\n"
                f"📝 Изоҳ: {note}\n"
                f"👤 Шахс: {person}"
            )

        sort_time = start_time or end_time or sana_text
        text_result.append((sort_time, block, video_id))

    if not text_result:
        await message.reply_text(
            "Бу вақт оралиғида ремонт историяси топилмади.\n\n"
            "Бошқа даврни танланг:",
            reply_markup=history_period_keyboard()
        )
        return

    text_result.sort(key=lambda x: x[0])

    await message.reply_text("📚 Техника историяси:")

    for _, block, video_id in text_result[-20:]:
        await message.reply_text(block)

        if video_id:
            await message.reply_video_note(video_id)


async def notify_technadzor_for_check(context, car):
    kirgan, chiqqan = get_last_repair_pair(car)

    if not chiqqan:
        return

    text = f"🚛 Техника: {car}\n🚜 Тури: {get_car_type(car)}\n\n"

    if kirgan:
        text += (
            "🔴 РЕМОНТГА КИРГАН\n"
            f"📅 Сана ва вақт: {kirgan[10] if len(kirgan) > 10 else kirgan[1]}\n"
            f"⏱ КМ/Моточас: {kirgan[3] if len(kirgan) > 3 else ''}\n"
            f"🔧 Ремонт тури: {kirgan[4] if len(kirgan) > 4 else ''}\n"
            f"📝 Изоҳ: {clean_note(kirgan[6] if len(kirgan) > 6 else '')}\n"
            f"👤 Киритган: {kirgan[9] if len(kirgan) > 9 else ''}\n\n"
        )

    text += (
        "🟡 РЕМОНТДАН ЧИҚҚАН\n"
        f"📅 Сана ва вақт: {chiqqan[11] if len(chiqqan) > 11 else chiqqan[1]}\n"
        f"📝 Изоҳ: {clean_note(chiqqan[6] if len(chiqqan) > 6 else '')}\n"
        f"⏳ Кетган вақт: {chiqqan[12] if len(chiqqan) > 12 else ''}\n"
        f"👤 Чиқарган: {chiqqan[9] if len(chiqqan) > 9 else ''}"
    )

    for user_id in get_user_ids_by_role("technadzor"):
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="🔔 Янги техника текширувга келди:"
            )

            await context.bot.send_message(
                chat_id=user_id,
                text=text
            )

            if kirgan and len(kirgan) > 7 and kirgan[7]:
                await context.bot.send_video_note(
                    chat_id=user_id,
                    video_note=kirgan[7]
                )

            if len(chiqqan) > 7 and chiqqan[7]:
                await context.bot.send_video_note(
                    chat_id=user_id,
                    video_note=chiqqan[7]
                )

            await context.bot.send_message(
                chat_id=user_id,
                text="Текширув натижасини танланг:",
                reply_markup=confirm_action_keyboard(car)
            )

        except Exception:
            pass


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await deny(update)
        return

    if role == "technadzor":
        await update.message.reply_text(
            "🧑‍🔍 Текширувчи менюси:",
            reply_markup=technadzor_keyboard()
        )
        return

    if role == "mechanic":
        await update.message.reply_text(
            "🔧 Механик менюси\n\nАввал фирмани танланг:",
            reply_markup=firm_keyboard()
        )
        return

    if role == "slesar":
        await update.message.reply_text(
            "🛠 Слесарь менюси\n\nАввал фирмани танланг:",
            reply_markup=firm_keyboard()
        )
        return

    if role == "director":
        await update.message.reply_text(
            "👨‍💼 Директор менюси\n\nАввал фирмани танланг:",
            reply_markup=firm_keyboard()
        )
        return

    await update.message.reply_text(
        "Аввал фирмани танланг:",
        reply_markup=firm_keyboard()
    )


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
        row_id,
        current_time,
        car,
        km,
        amal,
        status,
        note,
        video_id,
        km_photo_id,
        added_by,
        repair_start_time,
        repair_end_time,
        repair_duration,
        executor_id
    ])

    if operation == "remove":
        await notify_technadzor_for_check(context, car)

    try:
        if hasattr(update_or_query, "callback_query") and update_or_query.callback_query:
            await update_or_query.callback_query.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    
    await message_obj.reply_text(
        f"✅ Маълумот сақланди.\n\n"
        f"🚛 Техника: {car}\n"
        f"📌 Ҳолат: {status}",
        reply_markup=technadzor_keyboard() if role == "technadzor" else action_keyboard()
    )

    saved_firm = context.user_data.get("firm")
    context.user_data.clear()

    if role != "technadzor":
        context.user_data["firm"] = saved_firm


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await deny(update)
        return

    text = update.message.text.strip()
    mode = context.user_data.get("mode")

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

        rows = remont_ws.get_all_values()[1:]
        notify_id = None

        for row in reversed(rows):
            if len(row) > 13 and row[2].strip().lower() == car.lower():
                if row[5].strip() == "Текширувда":
                    notify_id = row[13]
                    break

        if notify_id:
            try:
                await context.bot.send_message(
                    chat_id=int(notify_id),
                    text=(
                        f"❌ {car} текширувдан ўтмади.\n"
                        f"Сабаб: {reason}\n"
                        f"Текширувчи: {inspector}"
                    )
                )
            except Exception:
                pass

        await update.message.reply_text(
            f"❌ {car} Носоз ҳолатига қайтарилди.",
            reply_markup=cars_for_check_by_firm_group()
        )

        context.user_data["mode"] = None
        context.user_data["reject_car"] = None
        return

    if mode == "edit_note":
        context.user_data["note"] = text
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            "✅ Изоҳ янгиланди.\n\n🔴 <b>Маълумотни тасдиқлайсизми?</b>",
            reply_markup=final_confirm_keyboard(),
            parse_mode="HTML"
        )
        return

    if text == "⬅️ Орқага":
        if role == "technadzor":
            context.user_data.clear()
        await update.message.reply_text(
            "Текширувчи менюси:",
            reply_markup=technadzor_keyboard()
        )
        return

        if mode == "choose_repair_type":
            context.user_data["mode"] = None
            await update.message.reply_text(
                "Амални танланг:",
                reply_markup=action_keyboard()
            )
            return

        context.user_data.clear()
        await update.message.reply_text(
            "Фирмани танланг:",
            reply_markup=firm_keyboard()
        )
        return

    if role == "technadzor":
        if text == "🔧 Ремонтга қўшиш":
            context.user_data.clear()
            context.user_data["mode"] = "select_firm_for_add"

            await update.message.reply_text(
                "🔴 <b>Фирмани танланг:</b>",
                parse_mode="HTML"
            )
            return

        if text == "☑️ Ремонтдан чиқишини тасдиқлаш":
            context.user_data["mode"] = "confirm_exit"

            await update.message.reply_text(
                "Текширувда турган техникалар:",
                reply_markup=cars_for_check_by_firm_group()
            )
            return

        if text == "📚 История":
            context.user_data["mode"] = "history_select_firm"

            await update.message.reply_text(
                "Қайси фирма техникасини кўрмоқчисиз?",
                reply_markup=firm_keyboard()
            )
            return

    if mode == "history_select_firm" and text in FIRM_NAMES:
        context.user_data["firm"] = text
        context.user_data["mode"] = "history_select_car"

        await update.message.reply_text(
            "⬅️ Орқага қайтиш учун пастдаги тугмани босинг.",
            reply_markup=back_keyboard()
        )

        await update.message.reply_text(
            "Техникани танланг:",
            reply_markup=car_buttons_by_firm(text)
        )
        return

    if text in FIRM_NAMES:
        if mode == "select_firm_for_add":
            context.user_data["firm"] = text
            context.user_data["mode"] = "choose_repair_type"
            context.user_data["operation"] = "add"

            await update.message.reply_text(
                "Ремонт турини танланг:",
                reply_markup=repair_type_keyboard()
            )
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

        await update.message.reply_text(
            "Ремонт турини танланг:",
            reply_markup=repair_type_keyboard()
        )
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
            "Техникани танланг:",
            reply_markup=car_buttons_by_firm(context.user_data["firm"])
        )
        return

    if mode == "write_km":
        context.user_data["km"] = text
        context.user_data["mode"] = "send_km_photo"

        await update.message.reply_text(
            "✅ КМ/моточас сақланди.\n\n"
            "Энди одометр ёки моточас расмини юборинг."
        )
        return

    if mode in ["write_note_add", "write_note_remove"]:
        context.user_data["note"] = text
        context.user_data["mode"] = "send_video"

        await update.message.reply_text(
            "✅ Изоҳ сақланди.\n\nЭнди думалоқ видео отчет юборинг."
        )
        return

    await update.message.reply_text("Менюдан тугмани танланг.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "none":
        return

    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await query.message.reply_text("❌ Сизга рухсат йўқ")
        return

    if query.data == "final_confirm":
        await save_final_data(update, context, query.message)

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except:
            pass

        await query.answer("Сақланди ✅")
        return

    if query.data == "final_edit":
        await query.edit_message_reply_markup(reply_markup=None)

        if context.user_data.get("operation") == "remove":
            keyboard = edit_keyboard_remove()
        else:
            keyboard = edit_keyboard()

        await query.message.reply_text(
            "🔴 <b>Қайси маълумотни таҳрирлайсиз?</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        return
 
    if query.data.startswith("car|"):
        car = query.data.split("|", 1)[1]
        mode = context.user_data.get("mode")

        if mode == "choose_car":
            context.user_data["car"] = car
            repair_type = context.user_data.get("repair_type")

            await send_last_repairs(query, car, repair_type)

            context.user_data["mode"] = "write_km"

            await query.message.reply_text(
                f"🚛 Техника: {car}\n"
                f"🏢 Фирма: {context.user_data.get('firm')}\n"
                f"🔧 Ремонт тури: {repair_type}\n\n"
                "🔴 <b>Юрган масофа ёки моточасни киритинг:</b>",
                parse_mode="HTML"
            )
            return
    if query.data.startswith("edit|"):
        field = query.data.split("|", 1)[1]

        if field == "km":
            context.user_data["mode"] = "edit_km"
            await query.message.reply_text("Янги КМ/моточасни ёзинг:")
            return

        if field == "photo":
            context.user_data["mode"] = "edit_photo"
            await query.message.reply_text("Янги расмни юборинг:")
            return

        if field == "note":
            context.user_data["mode"] = "edit_note"
            await query.message.reply_text("Янги изоҳни ёзинг:")
            return

        if field == "video":
            context.user_data["mode"] = "edit_video"
            await query.message.reply_text("Янги думалоқ видеони юборинг:")
            return

    if query.data.startswith("period|"):
        period = query.data.split("|", 1)[1]
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

        text = f"🚛 Техника: {car}\n🚜 Тури: {get_car_type(car)}\n\n"

        if kirgan_list:
           text += "🔴 РЕМОНТГА КИРГАНЛАР:\n\n"

           for kirgan in kirgan_list:
               text += (
            f"📅 {kirgan[10] if len(kirgan) > 10 else kirgan[1]}\n"
            f"⏱ {kirgan[3] if len(kirgan) > 3 else ''}\n"
            f"🔧 {kirgan[4] if len(kirgan) > 4 else ''}\n"
            f"📝 {clean_note(kirgan[6] if len(kirgan) > 6 else '')}\n\n"
        )

        text += (
            "🟡 РЕМОНТДАН ЧИҚҚАН\n"
            f"📅 Сана ва вақт: {chiqqan[11] if len(chiqqan) > 11 else chiqqan[1]}\n"
            f"📝 Изоҳ: {clean_note(chiqqan[6] if len(chiqqan) > 6 else '')}\n"
            f"⏳ Кетган вақт: {chiqqan[12] if len(chiqqan) > 12 else ''}\n"
            f"👤 Чиқарган: {chiqqan[9] if len(chiqqan) > 9 else ''}"
        )

        await query.message.reply_text(text)

        if kirgan and len(kirgan) > 7 and kirgan[7]:
            await query.message.reply_video_note(kirgan[7])

        if len(chiqqan) > 7 and chiqqan[7]:
            await query.message.reply_video_note(chiqqan[7])

        await query.message.reply_text(
            "Текширув натижасини танланг:",
            reply_markup=confirm_action_keyboard(car)
        )
        return

    if query.data.startswith("approve|"):
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

        await query.message.reply_text(
            f"✅ {car} соз деб тасдиқланди.\nҲолат: Соз",
            reply_markup=cars_for_check_by_firm_group()
        )
        return

    if query.data.startswith("reject|"):
        car = query.data.split("|", 1)[1]
        context.user_data["reject_car"] = car
        context.user_data["mode"] = "reject_reason"

        await query.message.reply_text(
            f"❌ {car} текширувдан ўтмади.\n\nСабабини ёзинг:"
        )
        return

    if not query.data.startswith("car|"):
        return

        car = query.data.split("|", 1)[1]
        mode = context.user_data.get("mode")

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
            "Мисол:\n"
            "125000 км\n"
            "ёки\n"
            "8500 моточас",
            parse_mode="HTML"
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
            "Юрган масофа ёки ишлаган соатни ёзинг.\n\n"
            "Мисол:\n"
            "125000 км\n"
            "ёки\n"
            "8500 моточас"
        )
        return

    if mode == "remove_car":
        context.user_data["car"] = car
        context.user_data["mode"] = "write_note_remove"

        await query.message.reply_text(
            f"🚛 Техника: {car}\n"
            f"🏢 Фирма: {context.user_data.get('firm')}\n\n"
            "Қилинган иш бўйича изоҳ ёзинг:"
        )
        return


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await deny(update)
        return

    mode = context.user_data.get("mode")

    if mode == "edit_photo":
        context.user_data["km_photo_id"] = update.message.photo[-1].file_id
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            "✅ Расм янгиланди.",
            reply_markup=final_confirm_keyboard()
        )
        return

    if mode != "send_km_photo":
        await update.message.reply_text("Фото фақат КМ/моточас босқичида қабул қилинади.")
        return

    context.user_data["km_photo_id"] = update.message.photo[-1].file_id
    context.user_data["mode"] = "write_note_add"

    await update.message.reply_text(
        "✅ КМ/моточас расми сақланди.\n\n"
        "Энди носозлик ёки изоҳни ёзинг:"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = get_role(update)

    if role not in ["director", "mechanic", "technadzor", "slesar"]:
        await deny(update)
        return

    mode = context.user_data.get("mode")

    if mode == "edit_video":
        context.user_data["video_id"] = update.message.video_note.file_id
        context.user_data["mode"] = "final_check"

        await update.message.reply_text(
            "✅ Видео янгиланди.\n\n🔴 <b>Маълумотни тасдиқлайсизми?</b>",
            reply_markup=final_confirm_keyboard(),
            parse_mode="HTML"
        )
        return

    if mode != "send_video":
        await update.message.reply_text(
            "Аввал фирма → амал → техника → изоҳ/КМ босқичларини бажаринг."
        )
        return

    context.user_data["video_id"] = update.message.video_note.file_id
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


app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(handle_callback))
app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
app.add_handler(MessageHandler(filters.VIDEO_NOTE, handle_video))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

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

threading.Thread(target=run_server, daemon=True).start()

app.run_polling()
