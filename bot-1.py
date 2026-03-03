#!/usr/bin/env python3
"""
🤖 MEGA Telegram Bot - Тегалка + Рассылка + Админка
Автор: Claude
"""

import logging
import asyncio
import json
import os
import sys
import random
from datetime import datetime, timedelta
from collections import defaultdict

from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMember, ChatPermissions
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.constants import ParseMode

# ======================== ЗАГРУЗКА .env ========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("❌ BOT_TOKEN не найден! Создай файл .env на основе .env.example")
    sys.exit(1)

# ID главных супер-админов бота (через запятую в .env)
_admins_raw = os.getenv("SUPER_ADMINS", "")
SUPER_ADMINS = [int(x.strip()) for x in _admins_raw.split(",") if x.strip().isdigit()]
if not SUPER_ADMINS:
    print("⚠️  SUPER_ADMINS не указан в .env — команды супер-админа будут недоступны")

# Файл для хранения данных
DATA_FILE = "bot_data.json"

# ======================== ЛОГИРОВАНИЕ ========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ======================== БАЗА ДАННЫХ (JSON) ========================
def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "groups": {},       # chat_id -> {"members": [], "settings": {}}
        "broadcasts": [],   # история рассылок
        "stats": {},        # статистика команд
        "banned": [],       # забаненные пользователи
        "scheduled": [],    # запланированные сообщения
        "welcomes": {},     # приветствия для групп
        "warns": {},        # предупреждения пользователей
    }

def save_data(data: dict):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

data = load_data()


# ======================== ХЕЛПЕРЫ ========================
def get_group(chat_id: int) -> dict:
    key = str(chat_id)
    if key not in data["groups"]:
        data["groups"][key] = {
            "members": [],
            "settings": {
                "tag_limit": 5,
                "broadcast_enabled": True,
                "welcome_enabled": False,
                "antispam": False,
                "mute_on_join": False,
            },
            "title": ""
        }
    return data["groups"][key]

def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if is_super_admin(user_id):
        return True
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, user_id)
        return member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
    except:
        return False

def bump_stat(cmd: str):
    data["stats"][cmd] = data["stats"].get(cmd, 0) + 1
    save_data(data)

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ======================== /start ========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("start")
    text = (
        "👋 *Привет! Я Mega Tag Bot* 🚀\n\n"
        "Я умею тегать всех участников группы, делать рассылки и многое другое!\n\n"
        "📋 *Основные команды:*\n"
        "`/all [текст]` — тегнуть всех участников\n"
        "`/broadcast [текст]` — рассылка по всем группам\n"
        "`/admins` — тегнуть всех админов\n"
        "`/stats` — статистика бота\n"
        "`/admin` — панель управления (для админов)\n\n"
        "➕ Добавь меня в группу и дай права администратора!"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Помощь", callback_data="help"),
         InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")]
    ])
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


# ======================== /all — ТЕГАЛКА ========================
async def cmd_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("all")
    chat = update.effective_chat
    user = update.effective_user

    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("❌ Эта команда работает только в группах!")
        return

    if str(user.id) in data.get("banned", []):
        return

    group = get_group(chat.id)
    members = group.get("members", [])

    if not members:
        await update.message.reply_text(
            "👤 Список участников пуст!\n"
            "Бот запоминает участников по мере их активности в чате.\n"
            "Используй /scan для принудительного сканирования (нужны права)."
        )
        return

    # Текст после команды
    extra_text = " ".join(context.args) if context.args else ""
    tag_limit = group["settings"].get("tag_limit", 5)

    # Фильтруем — убираем самого бота
    bot_id = context.bot.id
    members_filtered = [m for m in members if m["id"] != bot_id]

    chunks = list(chunk_list(members_filtered, tag_limit))
    total = len(members_filtered)

    # Шапка
    header = f"📢 *{extra_text}*\n\n" if extra_text else "📢 *Внимание всем!*\n\n"

    sent_count = 0
    for i, chunk in enumerate(chunks):
        tags = " ".join([
            f"@{m["username"]}" if m.get("username")
            else f"[{m.get('name', 'User')}](tg://user?id={m["id"]})" 
            for m in chunk
        ])

        if i == 0:
            msg = header + tags
        else:
            msg = tags

        try:
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            sent_count += len(chunk)
            if len(chunks) > 1:
                await asyncio.sleep(0.5)  # антифлуд
        except Exception as e:
            logger.error(f"Ошибка при отправке тегов: {e}")

    # Итог
    await update.message.reply_text(
        f"✅ Тегнуто *{sent_count}* из *{total}* участников в *{len(chunks)}* сообщениях",
        parse_mode=ParseMode.MARKDOWN
    )


# ======================== /admins — ТЕГНУТЬ АДМИНОВ ========================
async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("admins")
    chat = update.effective_chat

    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("❌ Только для групп!")
        return

    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        text = " ".join([
            f"[{a.user.first_name}](tg://user?id={a.user.id})"
            for a in admins if not a.user.is_bot
        ])
        extra = " ".join(context.args) if context.args else "Нужна ваша помощь!"
        await update.message.reply_text(
            f"👮 *Вызов администраторов!*\n\n{text}\n\n_{extra}_",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# ======================== /tag — ТЕГНУТЬ КОНКРЕТНОГО ========================
async def cmd_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("tag")
    if not context.args:
        await update.message.reply_text("Использование: `/tag @username [текст]`", parse_mode=ParseMode.MARKDOWN)
        return
    username = context.args[0]
    text = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    msg = f"👉 {username}"
    if text:
        msg += f" — {text}"
    await update.message.reply_text(msg)


# ======================== /broadcast — РАССЫЛКА ========================
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("broadcast")
    user = update.effective_user

    if not is_super_admin(user.id):
        await update.message.reply_text("❌ Только супер-админы могут делать рассылку!")
        return

    if not context.args:
        await update.message.reply_text(
            "📨 Использование: `/broadcast [текст]`\n\n"
            "Например: `/broadcast Привет! Обновление бота!`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    message_text = " ".join(context.args)
    groups = data["groups"]
    success = 0
    failed = 0

    status_msg = await update.message.reply_text("📤 Начинаю рассылку...")

    for chat_id_str, group_data in groups.items():
        if not group_data["settings"].get("broadcast_enabled", True):
            continue
        try:
            await context.bot.send_message(
                chat_id=int(chat_id_str),
                text=f"📢 *Сообщение от администрации:*\n\n{message_text}",
                parse_mode=ParseMode.MARKDOWN
            )
            success += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"Broadcast failed for {chat_id_str}: {e}")
            failed += 1

    # Логируем рассылку
    data["broadcasts"].append({
        "text": message_text,
        "time": datetime.now().isoformat(),
        "by": user.id,
        "success": success,
        "failed": failed
    })
    save_data(data)

    await status_msg.edit_text(
        f"✅ *Рассылка завершена!*\n\n"
        f"📤 Отправлено: *{success}* групп\n"
        f"❌ Ошибки: *{failed}* групп",
        parse_mode=ParseMode.MARKDOWN
    )


# ======================== /stats — СТАТИСТИКА ========================
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("stats")
    total_groups = len(data["groups"])
    total_broadcasts = len(data["broadcasts"])
    total_members = sum(len(g.get("members", [])) for g in data["groups"].values())

    stats_text = "\n".join([f"  `/{k}` — {v} раз" for k, v in sorted(data["stats"].items(), key=lambda x: -x[1])[:10]])

    text = (
        f"📊 *Статистика бота*\n\n"
        f"👥 Групп: *{total_groups}*\n"
        f"👤 Участников в БД: *{total_members}*\n"
        f"📨 Рассылок: *{total_broadcasts}*\n\n"
        f"🏆 *Топ команд:*\n{stats_text or 'Пока нет данных'}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ======================== /warn — ПРЕДУПРЕЖДЕНИЕ ========================
async def cmd_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("warn")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("↩️ Ответь на сообщение пользователя!")
        return

    target = update.message.reply_to_message.from_user
    reason = " ".join(context.args) if context.args else "Нарушение правил"
    key = str(target.id)

    if key not in data["warns"]:
        data["warns"][key] = []
    data["warns"][key].append({
        "reason": reason,
        "time": datetime.now().isoformat(),
        "chat": update.effective_chat.id
    })
    warn_count = len(data["warns"][key])
    save_data(data)

    await update.message.reply_text(
        f"⚠️ [{target.first_name}](tg://user?id={target.id}) получил предупреждение!\n"
        f"📝 Причина: {reason}\n"
        f"🔢 Всего предупреждений: *{warn_count}/3*\n"
        f"{'🚫 Будет заблокирован!' if warn_count >= 3 else ''}",
        parse_mode=ParseMode.MARKDOWN
    )

    if warn_count >= 3:
        try:
            await context.bot.ban_chat_member(update.effective_chat.id, target.id)
            await update.message.reply_text(f"🚫 {target.first_name} автоматически забанен за 3 предупреждения!")
        except:
            pass


# ======================== /mute — МУТ ========================
async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("mute")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("↩️ Ответь на сообщение пользователя!")
        return

    target = update.message.reply_to_message.from_user
    duration = int(context.args[0]) if context.args else 60  # минуты

    until = datetime.now() + timedelta(minutes=duration)
    try:
        await context.bot.restrict_chat_member(
            update.effective_chat.id,
            target.id,
            ChatPermissions(can_send_messages=False),
            until_date=until
        )
        await update.message.reply_text(
            f"🔇 [{target.first_name}](tg://user?id={target.id}) замучен на *{duration}* минут",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# ======================== /unmute ========================
async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_group_admin(update, context):
        return
    if not update.message.reply_to_message:
        return
    target = update.message.reply_to_message.from_user
    try:
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_polls=True, can_send_other_messages=True
            )
        )
        await update.message.reply_text(f"🔊 {target.first_name} размьючен!")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


# ======================== /kick ========================
async def cmd_kick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("kick")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("↩️ Ответь на сообщение!")
        return
    target = update.message.reply_to_message.from_user
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, target.id)
        await context.bot.unban_chat_member(update.effective_chat.id, target.id)
        await update.message.reply_text(f"👢 {target.first_name} кикнут из группы!")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


# ======================== /ban ========================
async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("ban")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("↩️ Ответь на сообщение!")
        return
    target = update.message.reply_to_message.from_user
    reason = " ".join(context.args) if context.args else "Без причины"
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, target.id)
        await update.message.reply_text(
            f"🚫 [{target.first_name}](tg://user?id={target.id}) забанен!\n📝 Причина: {reason}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


# ======================== /poll — ГОЛОСОВАНИЕ ========================
async def cmd_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("poll")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: `/poll Вопрос? Вариант1 Вариант2 ...`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    question = context.args[0]
    options = context.args[1:]
    try:
        await context.bot.send_poll(
            update.effective_chat.id,
            question=question,
            options=options[:10],
            is_anonymous=False
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


# ======================== /schedule — ЗАПЛАНИРОВАТЬ СООБЩЕНИЕ ========================
async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("schedule")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: `/schedule [минуты] [текст]`\n"
            "Пример: `/schedule 30 Напоминание через 30 минут!`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    try:
        delay = int(context.args[0])
        text = " ".join(context.args[1:])
        chat_id = update.effective_chat.id

        data["scheduled"].append({
            "chat_id": chat_id,
            "text": text,
            "send_at": (datetime.now() + timedelta(minutes=delay)).isoformat(),
            "done": False
        })
        save_data(data)

        await update.message.reply_text(
            f"⏰ Сообщение запланировано через *{delay}* минут!\n📝 Текст: _{text}_",
            parse_mode=ParseMode.MARKDOWN
        )

        # Запускаем таймер
        async def send_scheduled():
            await asyncio.sleep(delay * 60)
            try:
                await context.bot.send_message(chat_id, f"⏰ *Напоминание:*\n\n{text}", parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Scheduled msg error: {e}")

        asyncio.create_task(send_scheduled())
    except ValueError:
        await update.message.reply_text("❌ Укажи количество минут числом!")


# ======================== /setwelcome — ПРИВЕТСТВИЕ ========================
async def cmd_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("setwelcome")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return
    if not context.args:
        await update.message.reply_text(
            "Использование: `/setwelcome [текст]`\n"
            "Используй `{name}` для имени пользователя\n"
            "Пример: `/setwelcome Привет, {name}! Добро пожаловать!`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    welcome_text = " ".join(context.args)
    chat_id = str(update.effective_chat.id)
    group = get_group(update.effective_chat.id)
    group["settings"]["welcome_enabled"] = True
    data["welcomes"][chat_id] = welcome_text
    save_data(data)

    await update.message.reply_text(
        f"✅ Приветствие установлено!\n\nПредпросмотр:\n_{welcome_text.replace('{name}', update.effective_user.first_name)}_",
        parse_mode=ParseMode.MARKDOWN
    )


# ======================== /admin — ПАНЕЛЬ УПРАВЛЕНИЯ ========================
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("admin")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ У тебя нет прав администратора!")
        return

    chat = update.effective_chat
    is_super = is_super_admin(update.effective_user.id)

    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
         InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
        [InlineKeyboardButton("👥 Участники", callback_data="admin_members"),
         InlineKeyboardButton("📨 Рассылки", callback_data="admin_broadcasts")],
        [InlineKeyboardButton("⚠️ Предупреждения", callback_data="admin_warns"),
         InlineKeyboardButton("🔄 Сканировать", callback_data="admin_scan")],
    ]

    if is_super:
        keyboard.append([
            InlineKeyboardButton("🔴 Глобальная рассылка", callback_data="admin_global_broadcast"),
            InlineKeyboardButton("🚫 Управление банами", callback_data="admin_bans")
        ])

    keyboard.append([InlineKeyboardButton("❌ Закрыть", callback_data="close")])

    group = get_group(chat.id)
    members_count = len(group.get("members", []))

    text = (
        f"🎛️ *Панель управления*\n"
        f"{'👑 СУПЕР-АДМИН' if is_super else '👮 Админ'}\n\n"
        f"💬 Чат: *{chat.title or 'Личные сообщения'}*\n"
        f"👥 Участников в БД: *{members_count}*\n"
        f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ======================== /help ========================
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Полный список команд:*\n\n"
        "👥 *Тегалка:*\n"
        "`/all [текст]` — тегнуть всех участников\n"
        "`/admins [текст]` — тегнуть всех админов\n"
        "`/tag @user [текст]` — тегнуть конкретного\n\n"
        "📢 *Рассылка:*\n"
        "`/broadcast [текст]` — рассылка по всем группам (супер-адм)\n"
        "`/schedule [мин] [текст]` — запланировать сообщение\n\n"
        "👮 *Модерация:*\n"
        "`/warn [причина]` — предупреждение (реплай)\n"
        "`/mute [минуты]` — замутить (реплай)\n"
        "`/unmute` — размутить (реплай)\n"
        "`/kick` — кикнуть (реплай)\n"
        "`/ban [причина]` — забанить (реплай)\n\n"
        "🛠️ *Настройки:*\n"
        "`/setwelcome [текст]` — установить приветствие\n"
        "`/poll Вопрос? Вар1 Вар2` — создать голосование\n"
        "`/scan` — сканировать участников\n\n"
        "📊 *Инфо:*\n"
        "`/stats` — статистика\n"
        "`/admin` — панель управления\n"
        "`/help` — эта справка"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ======================== ОТСЛЕЖИВАНИЕ УЧАСТНИКОВ ========================
async def track_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запоминаем каждого, кто пишет в группу"""
    if update.effective_chat.type not in ["group", "supergroup"]:
        return

    user = update.effective_user
    if not user or user.is_bot:
        return

    group = get_group(update.effective_chat.id)
    group["title"] = update.effective_chat.title or ""

    member_data = {
        "id": user.id,
        "name": user.first_name or "User",
        "username": user.username or "",
        "last_seen": datetime.now().isoformat()
    }

    # Обновляем или добавляем
    existing = next((m for m in group["members"] if m["id"] == user.id), None)
    if existing:
        existing.update(member_data)
    else:
        group["members"].append(member_data)

    save_data(data)


# ======================== НОВЫЕ УЧАСТНИКИ ========================
async def on_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    for new_member in update.message.new_chat_members:
        if new_member.is_bot:
            continue

        # Добавляем в БД
        group = get_group(update.effective_chat.id)
        if not any(m["id"] == new_member.id for m in group["members"]):
            group["members"].append({
                "id": new_member.id,
                "name": new_member.first_name or "User",
                "username": new_member.username or "",
                "last_seen": datetime.now().isoformat()
            })
            save_data(data)

        # Приветствие
        if chat_id in data["welcomes"] and group["settings"].get("welcome_enabled"):
            welcome = data["welcomes"][chat_id].replace("{name}", new_member.first_name or "User")
            await update.message.reply_text(
                f"👋 [{new_member.first_name}](tg://user?id={new_member.id})\n\n{welcome}",
                parse_mode=ParseMode.MARKDOWN
            )


# ======================== /scan ========================
async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bump_stat("scan")
    if not await is_group_admin(update, context):
        await update.message.reply_text("❌ Только для админов!")
        return

    await update.message.reply_text(
        "🔍 Сканирование запущено!\n\n"
        "ℹ️ Telegram API не позволяет получить всех участников напрямую.\n"
        "Бот будет запоминать участников автоматически по мере их активности.\n\n"
        "📝 Для ускорения попроси всех написать что-нибудь в чат."
    )


# ======================== CALLBACK ОБРАБОТЧИКИ ========================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_cb = query.data

    if data_cb == "close":
        await query.message.delete()

    elif data_cb == "help":
        text = (
            "📖 *Основные команды:*\n\n"
            "`/all` — тегнуть всех\n"
            "`/admins` — тегнуть админов\n"
            "`/broadcast` — рассылка\n"
            "`/admin` — панель управления\n"
            "`/stats` — статистика\n"
            "`/help` — полный список"
        )
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_start")]]))

    elif data_cb == "show_stats":
        total_groups = len(data["groups"])
        total_members = sum(len(g.get("members", [])) for g in data["groups"].values())
        text = f"📊 *Статистика:*\n\nГрупп: {total_groups}\nУчастников в БД: {total_members}\nРассылок: {len(data['broadcasts'])}"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_start")]]))

    elif data_cb == "admin_stats":
        chat = query.message.chat
        group = get_group(chat.id)
        text = (
            f"📊 *Статистика группы*\n\n"
            f"💬 Чат: {chat.title}\n"
            f"👥 Участников в БД: {len(group.get('members', []))}\n"
            f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_admin")]]))

    elif data_cb == "admin_settings":
        chat = query.message.chat
        group = get_group(chat.id)
        s = group["settings"]
        keyboard = [
            [InlineKeyboardButton(
                f"{'✅' if s.get('broadcast_enabled', True) else '❌'} Рассылка",
                callback_data="toggle_broadcast"
            )],
            [InlineKeyboardButton(
                f"{'✅' if s.get('welcome_enabled', False) else '❌'} Приветствие",
                callback_data="toggle_welcome"
            )],
            [InlineKeyboardButton(f"🔢 Лимит тегов: {s.get('tag_limit', 5)}", callback_data="set_tag_limit")],
            [InlineKeyboardButton("◀️ Назад", callback_data="back_admin")]
        ]
        await query.edit_message_text(
            "⚙️ *Настройки группы:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data_cb == "toggle_broadcast":
        chat = query.message.chat
        group = get_group(chat.id)
        group["settings"]["broadcast_enabled"] = not group["settings"].get("broadcast_enabled", True)
        save_data(data)
        await handle_callback(update, context)  # обновить меню

    elif data_cb == "toggle_welcome":
        chat = query.message.chat
        group = get_group(chat.id)
        group["settings"]["welcome_enabled"] = not group["settings"].get("welcome_enabled", False)
        save_data(data)
        await handle_callback(update, context)

    elif data_cb == "admin_members":
        chat = query.message.chat
        group = get_group(chat.id)
        members = group.get("members", [])
        if members:
            preview = "\n".join([f"• {m['name']} (@{m.get('username','нет')})" for m in members[:15]])
            if len(members) > 15:
                preview += f"\n_...и ещё {len(members)-15}_"
        else:
            preview = "_Список пуст_"
        await query.edit_message_text(
            f"👥 *Участники ({len(members)}):*\n\n{preview}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_admin")]])
        )

    elif data_cb == "admin_broadcasts":
        broadcasts = data["broadcasts"][-5:]
        if broadcasts:
            text = "\n\n".join([
                f"📅 {b['time'][:16]}\n📝 {b['text'][:50]}...\n✅{b['success']} ❌{b['failed']}"
                for b in reversed(broadcasts)
            ])
        else:
            text = "_Рассылок ещё не было_"
        await query.edit_message_text(
            f"📨 *История рассылок:*\n\n{text}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_admin")]]))

    elif data_cb == "admin_warns":
        warns = data.get("warns", {})
        if warns:
            text = "\n".join([f"• ID {uid}: {len(w)} предупр." for uid, w in list(warns.items())[:10]])
        else:
            text = "_Предупреждений нет_"
        await query.edit_message_text(
            f"⚠️ *Предупреждения:*\n\n{text}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_admin")]]))

    elif data_cb == "back_admin":
        # Возврат в admin меню
        await query.edit_message_text(
            "🎛️ *Панель управления*\n\nВыбери раздел:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
                 InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
                [InlineKeyboardButton("👥 Участники", callback_data="admin_members"),
                 InlineKeyboardButton("📨 Рассылки", callback_data="admin_broadcasts")],
                [InlineKeyboardButton("⚠️ Предупреждения", callback_data="admin_warns")],
                [InlineKeyboardButton("❌ Закрыть", callback_data="close")]
            ])
        )

    elif data_cb == "back_start":
        await query.edit_message_text(
            "👋 *Mega Tag Bot*\n\nВыбери действие:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Помощь", callback_data="help"),
                 InlineKeyboardButton("📊 Статистика", callback_data="show_stats")],
                [InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")]
            ])
        )

    elif data_cb == "set_tag_limit":
        chat = query.message.chat
        group = get_group(chat.id)
        current = group["settings"].get("tag_limit", 5)
        options = [3, 5, 10, 15, 20]
        buttons = [InlineKeyboardButton(
            f"{'✅ ' if o == current else ''}{o}",
            callback_data=f"tag_limit_{o}"
        ) for o in options]
        await query.edit_message_text(
            f"🔢 *Текущий лимит: {current}*\nВыбери новый:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([buttons, [InlineKeyboardButton("◀️ Назад", callback_data="admin_settings")]])
        )

    elif data_cb.startswith("tag_limit_"):
        limit = int(data_cb.split("_")[-1])
        chat = query.message.chat
        group = get_group(chat.id)
        group["settings"]["tag_limit"] = limit
        save_data(data)
        await query.answer(f"✅ Лимит установлен: {limit}", show_alert=True)
        await handle_callback(update, context)  # вернуться в настройки


# ======================== ЗАПУСК ========================
def main():

    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("all", cmd_all))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("tag", cmd_tag))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("warn", cmd_warn))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("kick", cmd_kick))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("poll", cmd_poll))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("setwelcome", cmd_setwelcome))
    app.add_handler(CommandHandler("scan", cmd_scan))

    # Колбэки
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Отслеживание участников
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_member))

    # Новые участники
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_member))

    print("🤖 Бот запущен! Нажми Ctrl+C для остановки.")
    print("📋 Логи сохраняются в bot.log")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
