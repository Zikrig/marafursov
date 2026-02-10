import datetime as dt
from zoneinfo import ZoneInfo

import asyncio
from html import escape as _h
from aiogram import F, Router
from aiogram.filters import BaseFilter, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.types import BufferedInputFile
from aiogram.exceptions import TelegramBadRequest
from aiogram.enums import ParseMode
from sqlalchemy import select

from bot.config import Settings
from bot.db import (
    Progress,
    add_response,
    count_posts,
    delete_user_by_telegram_id,
    get_or_create_progress,
    get_post,
    get_post_by_position,
    get_responses_for_user,
    get_user_by_telegram_id,
    get_app_settings,
    count_responses_for_run,
    close_run_now,
    create_task_run,
    get_latest_open_run,
    get_latest_open_run_for_post,
    set_user_admin_flag,
    upsert_user,
)
from bot.keyboards import onboarding_go_kb, start_task_kb, summary_full_kb, task_done_kb

router = Router()

ONBOARDING_START_TEXT = (
    "Здравствуйте!\n"
    "Вас приветствует команда челленджа «30 дней для заявки». Давайте познакомимся!\n\n"
    "Укажите Ваше полное Ф.И.О."
)

DEFAULT_RULES_FALLBACK_TEXT = "Правила челленджа пока не настроены администратором."


class OnboardingFSM(StatesGroup):
    fio = State()
    region = State()
    email = State()


class NotCommand(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        txt = (message.text or message.caption or "").strip()
        return bool(txt) and not txt.startswith("/")


def _tznow(settings: Settings) -> dt.datetime:
    # Store/compare all timestamps as tz-naive "local time" in settings.tz
    return dt.datetime.now(ZoneInfo(settings.tz)).replace(tzinfo=None)


def _floor_to_minute(t: dt.datetime) -> dt.datetime:
    return t.replace(second=0, microsecond=0)


def _extract_text_or_caption(message: Message) -> str:
    return (message.text or message.caption or "").strip()


def _fmt_wait_minutes(m: int) -> str:
    """
    User-facing duration formatting:
    - if < 100 minutes -> show minutes
    - else -> show floor(hours) ("нацело на 60")
    """
    m = int(m)
    if m < 100:
        return f"{m} минут"
    h = m // 60
    return f"{h} часов"

async def _safe_send_html(message: Message, text: str, **kwargs) -> None:
    """
    Bot default parse_mode is HTML; if content contains unsupported tags (<...>),
    Telegram will reject. We fallback to plain text.
    """
    try:
        await message.answer(text, parse_mode=ParseMode.HTML, **kwargs)
    except TelegramBadRequest as e:
        if "can't parse entities" in str(e).lower():
            await message.answer(text, parse_mode=None, **kwargs)
            return
        raise


async def _safe_send_photo_with_caption(message: Message, *, file_id: str, caption: str, **kwargs) -> None:
    try:
        await message.answer_photo(photo=file_id, caption=caption, parse_mode=ParseMode.HTML, **kwargs)
    except TelegramBadRequest as e:
        if "can't parse entities" in str(e).lower():
            await message.answer_photo(photo=file_id, caption=caption, parse_mode=None, **kwargs)
            return
        raise

def _summary_text_for_post(*, post, responses) -> str:
    """
    Plain-text summary (no HTML) to avoid parse errors on arbitrary user content
    and to keep truncation safe.
    """
    title = (post.title or "").strip()
    body = f"День {post.position}. {title}\n\n"
    body += "Ответ(ы):\n"
    if responses:
        for r in responses:
            body += f"- {(r.text or '').strip()}\n"
    else:
        body += "- —\n"
    return body


async def _send_summary_item(message_like, *, post, responses, truncate_to: int = 500) -> None:
    """
    Sends one "question-answer" message per post.
    If too long, truncates to `truncate_to` chars and adds a button to show full.
    """
    full = _summary_text_for_post(post=post, responses=responses)
    if len(full) <= truncate_to:
        await message_like.answer(full, disable_web_page_preview=True, parse_mode=None)
        return
    short = full[: max(0, truncate_to - 1)].rstrip() + "…"
    await message_like.answer(
        short,
        disable_web_page_preview=True,
        reply_markup=summary_full_kb(post_id=post.id),
        parse_mode=None,
    )


async def _safe_send_task_notification(bot, *, chat_id: int, post) -> None:
    """
    Reusable "task received" message with HTML fallback (user content may break HTML).
    """
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=f"Вы получили сегодняшнее задание — <b>{_h(post.title or '')}</b>\n\nНачать?",
            reply_markup=start_task_kb(post_id=post.id),
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as e:
        if "can't parse entities" in str(e).lower():
            await bot.send_message(
                chat_id=chat_id,
                text=f"Вы получили сегодняшнее задание — {post.title}\n\nНачать?",
                reply_markup=start_task_kb(post_id=post.id),
                parse_mode=None,
            )
            return
        raise


async def _send_due_task_now(*, bot, session_factory, settings: Settings, telegram_id: int) -> str:
    """
    Send the next due task (same semantics as scheduler: one at a time, honor next_send_at).
    Returns a short status string for user feedback.
    """
    now = _tznow(settings)
    now_min = _floor_to_minute(now)

    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, telegram_id)
        if not user:
            return "no_user"
        if not getattr(user, "onboarded_at", None):
            return "not_onboarded"

        prog = get_or_create_progress(db, user_id=user.id, next_send_at=now_min)

        # If we are at the very beginning, allow immediate first task after "ПОЕХАЛИ!"
        if prog.next_position == 1 and prog.next_send_at and prog.next_send_at > now_min:
            prog.next_send_at = now_min
            prog.updated_at = dt.datetime.now()
            db.commit()

        if prog.pending_post_id:
            post = get_post(db, int(prog.pending_post_id))
            if post:
                await _safe_send_task_notification(bot, chat_id=telegram_id, post=post)
            return "already_pending"
        if prog.active_post_id:
            return "already_active"

        if prog.next_send_at and prog.next_send_at > now_min:
            return "too_early"

        max_posts = count_posts(db)
        if prog.next_position > max_posts:
            return "done"

        post = get_post_by_position(db, position=int(prog.next_position))
        if not post:
            return "missing_post"

        await _safe_send_task_notification(bot, chat_id=telegram_id, post=post)
        prog.pending_post_id = post.id
        prog.next_position += 1
        prog.updated_at = dt.datetime.now()
        db.commit()
        return "sent"
    finally:
        db.close()


@router.message(Command("start"))
async def cmd_start(message: Message, settings: Settings, session_factory, state: FSMContext):
    if not message.from_user:
        return

    now = _tznow(settings)
    now_min = _floor_to_minute(now)

    db = session_factory()
    try:
        user = upsert_user(db, telegram_id=message.from_user.id)
        set_user_admin_flag(db, telegram_id=user.telegram_id, is_admin=(user.telegram_id in settings.admin_ids))

        # /start always begins with onboarding сценарий
        await state.clear()
        await state.set_state(OnboardingFSM.fio)
        await message.answer(
            ONBOARDING_START_TEXT,
            disable_web_page_preview=True,
            parse_mode=None,
        )
        return
    finally:
        db.close()


@router.message(Command("cancel"), StateFilter(OnboardingFSM))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Ок, отменено.", parse_mode=None)


@router.message(OnboardingFSM.fio)
async def onboarding_fio(message: Message, settings: Settings, session_factory, state: FSMContext):
    if not message.from_user:
        return
    fio = _extract_text_or_caption(message)
    if len(fio) < 5:
        await message.answer("Пожалуйста, укажите полное Ф.И.О. (хотя бы 5 символов).")
        return
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            user = upsert_user(db, telegram_id=message.from_user.id)
        user.full_name = fio.strip()
        db.commit()
    finally:
        db.close()
    await state.set_state(OnboardingFSM.region)
    await message.answer("Укажите Ваш регион", parse_mode=None)


@router.message(OnboardingFSM.region)
async def onboarding_region(message: Message, settings: Settings, session_factory, state: FSMContext):
    if not message.from_user:
        return
    region = _extract_text_or_caption(message)
    if len(region) < 2:
        await message.answer("Пожалуйста, укажите регион (хотя бы 2 символа).")
        return
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            user = upsert_user(db, telegram_id=message.from_user.id)
        user.region = region.strip()
        db.commit()
    finally:
        db.close()
    await state.set_state(OnboardingFSM.email)
    await message.answer("Укажите Вашу электронную почту", parse_mode=None)


def _looks_like_email(s: str) -> bool:
    s = (s or "").strip()
    if " " in s:
        return False
    if s.count("@") != 1:
        return False
    local, domain = s.split("@", 1)
    if not local or "." not in domain:
        return False
    return True


@router.message(OnboardingFSM.email)
async def onboarding_email(message: Message, settings: Settings, session_factory, state: FSMContext):
    if not message.from_user:
        return
    email = _extract_text_or_caption(message)
    if not _looks_like_email(email):
        await message.answer("Пожалуйста, укажите корректный email (например: name@example.com).")
        return

    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            user = upsert_user(db, telegram_id=message.from_user.id)
        user.email = email.strip()
        db.commit()
    finally:
        db.close()

    # Send rules block (admin-editable greeting_text + optional image)
    db = session_factory()
    try:
        app = get_app_settings(db)
        rules_text = (app.greeting_text or "").strip() or DEFAULT_RULES_FALLBACK_TEXT
        if getattr(app, "greeting_media_type", None) == "photo" and getattr(app, "greeting_file_id", None):
            await _safe_send_photo_with_caption(
                message,
                file_id=str(app.greeting_file_id),
                caption=rules_text,
                disable_web_page_preview=True,
                reply_markup=onboarding_go_kb(),
            )
        else:
            await _safe_send_html(message, rules_text, disable_web_page_preview=True, reply_markup=onboarding_go_kb())
    finally:
        db.close()

    await state.clear()


@router.callback_query(F.data == "onboarding:go")
async def onboarding_go_callback(call: CallbackQuery, settings: Settings, session_factory):
    if not call.from_user:
        return
    await call.answer("Поехали!")
    # Make the button disappear; ignore if message can't be edited (e.g. old).
    if call.message:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass

    # Mark onboarding completed ONLY after "ПОЕХАЛИ!"
    now = _tznow(settings)
    now_min = _floor_to_minute(now)
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, call.from_user.id)
        if not user:
            user = upsert_user(db, telegram_id=call.from_user.id)
        if not getattr(user, "onboarded_at", None):
            user.onboarded_at = now_min
            db.commit()
    finally:
        db.close()

    status = await _send_due_task_now(
        bot=call.bot,
        session_factory=session_factory,
        settings=settings,
        telegram_id=call.from_user.id,
    )
    if status == "sent":
        return
    if status == "already_pending":
        return
    if status == "already_active":
        await call.bot.send_message(call.from_user.id, "У вас уже есть активное задание. Просто отправляйте ответы в чат.")
        return
    if status == "too_early":
        await call.bot.send_message(call.from_user.id, "Следующее задание будет доступно позже по таймеру.")
        return
    if status == "missing_post":
        await call.bot.send_message(call.from_user.id, "Не нашёл задание в базе. Сообщите администратору.")
        return
    if status == "done":
        await call.bot.send_message(call.from_user.id, "Похоже, задания закончились. Нажмите «Посмотреть мои ответы» в финале.")
        return
    # no_user / not_onboarded / unexpected
    await call.bot.send_message(call.from_user.id, "Не удалось выдать задание. Попробуйте ещё раз через минуту.")

@router.message(Command("null"))
async def cmd_null(message: Message, settings: Settings, session_factory):
    """
    Forget the user: delete user + progress + responses (via cascade).
    """
    if not message.from_user:
        return
    db = session_factory()
    try:
        ok = delete_user_by_telegram_id(db, message.from_user.id)
    finally:
        db.close()
    await message.answer("✅ Сброшено." if ok else "Пользователь не найден.")


@router.message(Command("summary"))
async def cmd_summary(message: Message, settings: Settings, session_factory):
    """
    Get summary on demand (useful for debugging; final-day flow still uses the button).
    """
    if not message.from_user:
        return
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            await message.answer("Пользователь не найден.")
            return
        items = get_responses_for_user(db, user_id=user.id)
    finally:
        db.close()

    if not items:
        await message.answer("Пока нет заданий или ответов.")
        return
    await message.answer("<b>Ваши ответы по дням</b>", disable_web_page_preview=True)
    for post, responses in items:
        await _send_summary_item(message, post=post, responses=responses, truncate_to=500)


@router.callback_query(F.data.startswith("task:start:"))
async def start_task_callback(call: CallbackQuery, settings: Settings, session_factory):
    if not call.from_user:
        return
    post_id = int(call.data.split(":", 2)[2])

    now = _tznow(settings)
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, call.from_user.id)
        if not user:
            user = upsert_user(db, telegram_id=call.from_user.id)

        post = get_post(db, post_id)
        if not post:
            await call.answer("Задание не найдено.", show_alert=True)
            return

        prog = get_or_create_progress(db, user_id=user.id, next_send_at=now)

        # If a run for this post is already open, do not "restart the timer".
        existing_open = get_latest_open_run_for_post(db, user_id=user.id, post_id=post.id, now=now)

        app = get_app_settings(db)
        if existing_open:
            until = existing_open.until
        else:
            until = now + dt.timedelta(minutes=int(app.response_window_minutes))
            create_task_run(db, user_id=user.id, post_id=post.id, started_at=now, until=until)

        window_text = _fmt_wait_minutes(int(app.response_window_minutes))
        # Keep Progress in sync (for status UI + "one active task" guard)
        if prog.pending_post_id == post.id:
            prog.pending_post_id = None
        prog.active_post_id = post.id
        prog.active_started_at = now
        prog.active_until = until
        prog.updated_at = dt.datetime.now()
        db.commit()

        # render day number procedurally (position)
        text = (
            f"<b>День {post.position}. {_h(post.title)}</b>\n\n"
            f"{post.text_html}\n\n"
            f"<b>Важно:</b> у вас есть <b>{window_text}</b> на выполнение задания с момента нажатия кнопки.\n"
            f"Можно отправить до <b>{settings.max_responses_per_task}</b> сообщений."
        )

        # remove keyboard from previous message
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        # send media if configured
        if post.media_type == "photo" and post.file_id:
            await call.message.answer_photo(photo=post.file_id, caption=text)
        else:
            await call.message.answer(text, disable_web_page_preview=True)
        await call.answer("Ок ✅")
    finally:
        db.close()


@router.message(F.chat.type == "private", NotCommand(), StateFilter(None), ~F.reply_to_message)
async def capture_user_answer(message: Message, settings: Settings, session_factory):
    if not message.from_user:
        return

    txt = _extract_text_or_caption(message)
    if not txt:
        return

    now = _tznow(settings)
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            return

        # 1) If user replied to a bot question message, route to that day
        target_run = None
        if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.is_bot:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            # expecting "День X." at the beginning
            import re

            m = re.match(r"^День\s+(\d+)", replied_text)
            if m:
                pos = int(m.group(1))
                post = get_post_by_position(db, position=pos)
                if post:
                    target_run = get_latest_open_run_for_post(db, user_id=user.id, post_id=post.id, now=now)

        # 2) Otherwise: latest open task
        if not target_run:
            target_run = get_latest_open_run(db, user_id=user.id, now=now)

        if not target_run:
            return

        # limit to 3 messages per task run
        current_cnt = count_responses_for_run(db, run_id=target_run.id)
        if current_cnt >= settings.max_responses_per_task:
            # silently close when limit reached
            close_run_now(db, run_id=target_run.id, now=now)
            return

        post = get_post(db, target_run.post_id)
        if not post:
            return

        add_response(db, run_id=target_run.id, user_id=user.id, post_id=post.id, text=txt)

        app = get_app_settings(db)
        interval_text = _fmt_wait_minutes(int(app.send_interval_minutes))
        remaining = max(0, int(settings.max_responses_per_task) - (current_cnt + 1))

        # if this was the last allowed answer -> close and schedule next from close time
        if current_cnt + 1 >= settings.max_responses_per_task:
            close_run_now(db, run_id=target_run.id, now=now)
            prog = db.scalar(select(Progress).where(Progress.user_id == user.id))
            if prog:
                prog.active_post_id = None
                prog.active_started_at = None
                prog.active_until = None
                prog.pending_post_id = None
                prog.next_send_at = _floor_to_minute(now) + dt.timedelta(minutes=int(app.send_interval_minutes))
                prog.updated_at = dt.datetime.now()
                db.commit()
            await message.answer(
                f"Спасибо! Ваш ответ записан.\n"
                f"Задание закрыто.\n"
                f"Следующее задание станет доступным через {interval_text}.",
                parse_mode=None,
            )
            return

        await message.answer(
            f"Спасибо! Ваш ответ записан.\n"
            f"Можно отправить ещё {remaining} сообщ.\n"
            f"Следующее задание станет доступным через {interval_text} после завершения задания. Если задание завершено — нажмите кнопку ниже.",
            reply_markup=task_done_kb(post_id=post.id),
            parse_mode=None,
        )
    finally:
        db.close()


@router.message(F.chat.type == "private", NotCommand(), F.reply_to_message)
async def capture_user_answer_reply_always(message: Message, settings: Settings, session_factory):
    """
    Allow answering by replying to bot's "День X..." message even if admin FSM state is active.
    This prevents admin edit states from blocking normal marathon answering.
    """
    if not message.from_user:
        return
    if not message.reply_to_message or not message.reply_to_message.from_user or not message.reply_to_message.from_user.is_bot:
        return

    replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
    import re

    m = re.match(r"^День\s+(\d+)", replied_text)
    if not m:
        return

    txt = _extract_text_or_caption(message)
    if not txt:
        return

    pos = int(m.group(1))
    now = _tznow(settings)
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, message.from_user.id)
        if not user:
            return
        post = get_post_by_position(db, position=pos)
        if not post:
            return
        run = get_latest_open_run_for_post(db, user_id=user.id, post_id=post.id, now=now)
        if not run:
            return
        current_cnt = count_responses_for_run(db, run_id=run.id)
        if current_cnt >= settings.max_responses_per_task:
            close_run_now(db, run_id=run.id, now=now)
            return
        add_response(db, run_id=run.id, user_id=user.id, post_id=post.id, text=txt)

        app = get_app_settings(db)
        interval_text = _fmt_wait_minutes(int(app.send_interval_minutes))
        remaining = max(0, int(settings.max_responses_per_task) - (current_cnt + 1))

        if current_cnt + 1 >= settings.max_responses_per_task:
            close_run_now(db, run_id=run.id, now=now)
            prog = db.scalar(select(Progress).where(Progress.user_id == user.id))
            if prog:
                prog.active_post_id = None
                prog.active_started_at = None
                prog.active_until = None
                prog.pending_post_id = None
                prog.next_send_at = _floor_to_minute(now) + dt.timedelta(minutes=int(app.send_interval_minutes))
                prog.updated_at = dt.datetime.now()
                db.commit()
            await message.answer(
                f"Спасибо! Ваш ответ записан.\n"
                f"Задание закрыто.\n"
                f"Следующее задание станет доступным через {interval_text}.",
                parse_mode=None,
            )
            return

        await message.answer(
            f"Спасибо! Ваш ответ записан.\n"
            f"Можно отправить ещё {remaining} сообщ.\n"
            f"Следующее задание станет доступным через {interval_text} после завершения задания. Если задание завершено — нажмите кнопку ниже.",
            reply_markup=task_done_kb(post_id=post.id),
            parse_mode=None,
        )
    finally:
        db.close()


@router.callback_query(F.data.startswith("task:done:"))
async def task_done_callback(call: CallbackQuery, settings: Settings, session_factory):
    if not call.from_user:
        return
    post_id = int(call.data.split(":", 2)[2])
    now = _tznow(settings)
    now_min = _floor_to_minute(now)

    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, call.from_user.id)
        if not user:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        app = get_app_settings(db)
        interval_text = _fmt_wait_minutes(int(app.send_interval_minutes))

        run = get_latest_open_run_for_post(db, user_id=user.id, post_id=post_id, now=now)
        if not run:
            await call.answer("Задание уже закрыто или окно ответа истекло.", show_alert=True)
            return

        close_run_now(db, run_id=run.id, now=now)
        prog = db.scalar(select(Progress).where(Progress.user_id == user.id))
        if prog:
            if prog.active_post_id == post_id:
                prog.active_post_id = None
                prog.active_started_at = None
                prog.active_until = None
            prog.pending_post_id = None
            prog.next_send_at = now_min + dt.timedelta(minutes=int(app.send_interval_minutes))
            prog.updated_at = dt.datetime.now()
            db.commit()

        await call.message.answer(
            f"✅ Готово! Задание закрыто.\n"
            f"Следующее задание станет доступным через {interval_text}.",
            parse_mode=None,
        )
        await call.answer()
    finally:
        db.close()


@router.callback_query(F.data == "summary:show")
async def show_summary(call: CallbackQuery, settings: Settings, session_factory):
    if not call.from_user:
        return
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, call.from_user.id)
        if not user:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        items = get_responses_for_user(db, user_id=user.id)
    finally:
        db.close()

    if not items:
        await call.message.answer("Пока нет заданий или ответов.")
        await call.answer()
        return

    await call.message.answer("Ваши ответы по дням", disable_web_page_preview=True, parse_mode=None)
    for post, responses in items:
        await _send_summary_item(call.message, post=post, responses=responses, truncate_to=500)
    await call.answer()


@router.callback_query(F.data.startswith("summary:full:"))
async def show_summary_full(call: CallbackQuery, settings: Settings, session_factory):
    if not call.from_user:
        return
    post_id = int(call.data.split(":", 2)[2])
    db = session_factory()
    try:
        user = get_user_by_telegram_id(db, call.from_user.id)
        if not user:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        items = get_responses_for_user(db, user_id=user.id)
        found = None
        for post, responses in items:
            if post.id == post_id:
                found = (post, responses)
                break
    finally:
        db.close()

    if not found:
        await call.answer("Не найдено", show_alert=True)
        return

    post, responses = found
    full = _summary_text_for_post(post=post, responses=responses)

    # If too long for Telegram, send as file
    if len(full) > 3500:
        buf = BufferedInputFile(full.encode("utf-8"), filename=f"day_{post.position}.txt")
        await call.message.answer_document(buf, caption=f"День {post.position}. {(post.title or '').strip()}")
    else:
        await call.message.answer(full, disable_web_page_preview=True, parse_mode=None)
    await call.answer()
