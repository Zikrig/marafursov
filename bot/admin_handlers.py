import datetime as dt
import io
from zoneinfo import ZoneInfo

from html import escape as _h
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.types import BufferedInputFile
from openpyxl import Workbook
from sqlalchemy import func, select

from bot.config import Settings
from bot.db import (
    Post,
    Progress,
    Response,
    TaskRun,
    User,
    count_posts,
    create_post,
    delete_post,
    get_app_settings,
    get_or_create_progress,
    get_post,
    get_responses_for_user,
    get_user_by_telegram_id,
    list_posts,
    move_post,
    delete_task_runs_for_user,
    reset_progress,
    set_user_admin_flag,
    set_greeting_text,
    set_response_window_minutes,
    set_send_interval_minutes,
    upsert_user,
    update_post,
)
from bot.keyboards import (
    admin_edit_post_kb,
    admins_menu_kb,
    admins_posts_list_kb,
)

admin_router = Router()

PAGE_SIZE = 8


def _is_admin(user_id: int | None, settings: Settings) -> bool:
    return bool(user_id) and int(user_id) in settings.admin_ids


def _tznow(settings: Settings) -> dt.datetime:
    # Store/compare all timestamps as tz-naive "local time" in settings.tz
    return dt.datetime.now(ZoneInfo(settings.tz)).replace(tzinfo=None)


async def _render_admin_menu_text(*, telegram_id: int, session_factory) -> str:
    db = session_factory()
    try:
        s = get_app_settings(db)
        u = db.scalar(select(User).where(User.telegram_id == telegram_id))
        if not u:
            u = upsert_user(db, telegram_id=telegram_id)
            set_user_admin_flag(db, telegram_id=telegram_id, is_admin=True)
        # Ensure progress always exists for admins menu (never show "нет")
        now = dt.datetime.now().replace(second=0, microsecond=0)
        prog = get_or_create_progress(db, user_id=u.id, next_send_at=now)
        total_posts = count_posts(db)
    finally:
        db.close()

    def _fmt_prog(p: Progress | None) -> str:
        if not p:
            return "нет"
        done = max(0, p.next_position - 1)
        pending = f"pending_post_id={p.pending_post_id}" if p.pending_post_id else "pending=нет"
        active = f"active_post_id={p.active_post_id} до {p.active_until}" if p.active_post_id else "active=нет"
        return f"пройдено дней: <b>{done}</b>\n{pending}\n{active}\nnext_send_at: <code>{p.next_send_at}</code>"

    return (
        "<b>Админ-меню</b>\n\n"
        f"⏱ Окно ответа: <b>{s.response_window_minutes} мин</b>\n"
        f"⏲ Интервал рассылки: <b>{s.send_interval_minutes} мин</b>\n\n"
        f"<b>Постов в БД</b>: <b>{total_posts}</b>\n\n"
        "<b>Прогресс</b>\n" + _fmt_prog(prog)
    )


class AdminEditFSM(StatesGroup):
    title = State()
    text = State()
    media = State()
    create_title = State()
    create_text = State()
    create_media = State()
    greeting = State()
    response_window = State()
    send_interval = State()

async def _render_list(call: CallbackQuery, *, page: int, session_factory) -> None:
    db = session_factory()
    try:
        total = count_posts(db)
        posts = list_posts(db, limit=PAGE_SIZE, offset=page * PAGE_SIZE)
        items = [(p.id, p.position, p.title) for p in posts]
    finally:
        db.close()

    await call.message.edit_text(
        f"Посты (всего: <b>{total}</b>):",
        reply_markup=admins_posts_list_kb(posts=items, page=page, page_size=PAGE_SIZE, total=total),
    )


@admin_router.message(Command("admins"))
@admin_router.message(Command("admin"))
async def cmd_admins(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    await state.clear()
    text = await _render_admin_menu_text(telegram_id=message.from_user.id, session_factory=session_factory)
    await message.answer(text, reply_markup=admins_menu_kb())


@admin_router.message(Command("cancel"))
async def cmd_cancel(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    await state.clear()
    text = await _render_admin_menu_text(telegram_id=message.from_user.id, session_factory=session_factory)
    await message.answer("Отменено.\n\n" + text, reply_markup=admins_menu_kb())


@admin_router.callback_query(F.data == "admin:menu")
async def admin_menu(call: CallbackQuery, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    text = await _render_admin_menu_text(telegram_id=call.from_user.id, session_factory=session_factory)
    await call.message.edit_text(text, reply_markup=admins_menu_kb())
    await call.answer()


@admin_router.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()

@admin_router.callback_query(F.data == "admin:greeting")
async def admin_greeting(call: CallbackQuery, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    db = session_factory()
    try:
        s = get_app_settings(db)
        current = s.greeting_text
    finally:
        db.close()
    await state.clear()
    await state.set_state(AdminEditFSM.greeting)
    await call.message.answer(
        "<b>Приветствие</b>\n\nТекущее:\n"
        f"{_h(current)}\n\n"
        "Пришлите новый текст приветствия:",
        disable_web_page_preview=True,
    )
    await call.answer()


@admin_router.callback_query(F.data == "admin:resp_window")
async def admin_resp_window(call: CallbackQuery, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    db = session_factory()
    try:
        s = get_app_settings(db)
        current = s.response_window_minutes
    finally:
        db.close()
    await state.clear()
    await state.set_state(AdminEditFSM.response_window)
    await call.message.answer(
        f"Текущее окно ответа: <b>{current} мин</b>\n\n"
        "Пришлите новое значение (целое число минут, минимум 1):"
    )
    await call.answer()


@admin_router.message(AdminEditFSM.response_window)
async def admin_save_resp_window(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    raw = (message.text or "").strip()
    try:
        minutes = int(raw)
    except Exception:
        await message.answer(
            "Вы сейчас в настройках (ожидаю <b>число минут</b>).\n\n"
            "- **Отменить**: /cancel\n"
            "- **Ответить на задание**: отправьте ответ <b>реплаем</b> на сообщение «День X…»",
            disable_web_page_preview=True,
        )
        return
    db = session_factory()
    try:
        s = set_response_window_minutes(db, minutes=minutes)
        current = s.response_window_minutes
    finally:
        db.close()
    await state.clear()
    await message.answer(f"✅ Окно ответа установлено: <b>{current} мин</b>", reply_markup=admins_menu_kb())


@admin_router.callback_query(F.data == "admin:send_interval")
async def admin_send_interval(call: CallbackQuery, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    db = session_factory()
    try:
        s = get_app_settings(db)
        current = s.send_interval_minutes
    finally:
        db.close()
    await state.clear()
    await state.set_state(AdminEditFSM.send_interval)
    await call.message.answer(
        f"Текущий интервал рассылки: <b>{current} мин</b>\n\n"
        "Пришлите новое значение (целое число минут, минимум 1):"
    )
    await call.answer()


@admin_router.callback_query(F.data == "admin:summary:me")
async def admin_summary_me(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return

    db = session_factory()
    try:
        u = get_user_by_telegram_id(db, call.from_user.id)
        if not u:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        items = get_responses_for_user(db, user_id=u.id)
    finally:
        db.close()

    if not items:
        await call.message.answer("Пока нет заданий или ответов.")
        await call.answer()
        return

    await call.message.answer("<b>Ваша сводка ответов</b>", disable_web_page_preview=True)
    for post, responses in items:
        full = f"<b>День {post.position}. {_h(post.title)}</b>\n\n"
        if responses:
            full += "<b>Ответ(ы):</b>\n"
            for r in responses:
                full += f"- {_h(r.text)}\n"
        else:
            full += "<b>Ответ(ы):</b>\n- —\n"

        if len(full) <= 500:
            await call.message.answer(full, disable_web_page_preview=True)
        else:
            short = full[:499].rstrip() + "…"
            # In admin menu we don't need callbacks; send full as file to preserve data
            await call.message.answer(short, disable_web_page_preview=True)
            data = full.replace("<b>", "").replace("</b>", "")
            buf = BufferedInputFile(data.encode("utf-8"), filename=f"day_{post.position}.txt")
            await call.message.answer_document(buf, caption=f"День {post.position}. {_h(post.title)}")
    await call.answer()


def _truncate_excel_cell(s: str, limit: int = 32000) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 1)].rstrip() + "…"


@admin_router.callback_query(F.data == "admin:export:xlsx")
async def admin_export_all_summaries_xlsx(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id if call.from_user else None, settings):
        await call.answer("Нет доступа", show_alert=True)
        return

    await call.answer("Готовлю Excel…")

    db = session_factory()
    try:
        posts = list(db.scalars(select(Post).order_by(Post.position.asc(), Post.id.asc())))
        users = list(db.scalars(select(User).order_by(User.id.asc(), User.telegram_id.asc())))

        # Latest run per (user, post) by started_at
        latest = (
            select(
                TaskRun.user_id.label("user_id"),
                TaskRun.post_id.label("post_id"),
                func.max(TaskRun.started_at).label("max_started_at"),
            )
            .group_by(TaskRun.user_id, TaskRun.post_id)
            .subquery()
        )
        latest_runs = (
            select(TaskRun.id.label("run_id"), TaskRun.user_id.label("user_id"), TaskRun.post_id.label("post_id"))
            .join(
                latest,
                (TaskRun.user_id == latest.c.user_id)
                & (TaskRun.post_id == latest.c.post_id)
                & (TaskRun.started_at == latest.c.max_started_at),
            )
            .subquery()
        )

        rows = db.execute(
            select(latest_runs.c.user_id, latest_runs.c.post_id, Response.seq, Response.text)
            .join(Response, Response.run_id == latest_runs.c.run_id)
            .order_by(latest_runs.c.user_id.asc(), latest_runs.c.post_id.asc(), Response.seq.asc(), Response.id.asc())
        ).all()
    finally:
        db.close()

    answers: dict[tuple[int, int], list[str]] = {}
    for user_id, post_id, _seq, text in rows:
        answers.setdefault((int(user_id), int(post_id)), []).append(text or "")

    wb = Workbook()
    ws = wb.active
    ws.title = "summaries"

    headers = ["telegram_id"] + [f"День {p.position}. {p.title}" for p in posts]
    ws.append(headers)

    for u in users:
        row: list[object] = [int(u.telegram_id)]
        for p in posts:
            parts = answers.get((int(u.id), int(p.id)), [])
            cell = "\n".join([s.strip() for s in parts if (s or "").strip()])
            row.append(_truncate_excel_cell(cell))
        ws.append(row)

    ws.freeze_panes = "A2"

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f"summaries_{dt.datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
    await call.message.answer_document(BufferedInputFile(out.getvalue(), filename=filename))


@admin_router.message(AdminEditFSM.send_interval)
async def admin_save_send_interval(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    raw = (message.text or "").strip()
    try:
        minutes = int(raw)
    except Exception:
        await message.answer(
            "Вы сейчас в настройках (ожидаю <b>число минут</b>).\n\n"
            "- **Отменить**: /cancel\n"
            "- **Ответить на задание**: отправьте ответ <b>реплаем</b> на сообщение «День X…»",
            disable_web_page_preview=True,
        )
        return
    db = session_factory()
    try:
        s = set_send_interval_minutes(db, minutes=minutes)
        current = s.send_interval_minutes
    finally:
        db.close()
    await state.clear()
    await message.answer(f"✅ Интервал рассылки установлен: <b>{current} мин</b>", reply_markup=admins_menu_kb())

@admin_router.message(AdminEditFSM.greeting)
async def admin_save_greeting(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    txt = message.html_text or message.text or ""
    if not txt.strip():
        await message.answer("Текст пустой. Пришлите ещё раз:")
        return
    db = session_factory()
    try:
        set_greeting_text(db, text=txt)
    finally:
        db.close()
    await state.clear()
    await message.answer("✅ Приветствие обновлено.", reply_markup=admins_menu_kb())


@admin_router.callback_query(F.data.startswith("admin:list:"))
async def admin_list_posts(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, page_s = call.data.split(":", 2)
    page = int(page_s)
    await _render_list(call, page=page, session_factory=session_factory)
    await call.answer()


@admin_router.callback_query(F.data.startswith("admin:move:"))
async def admin_move_post(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, direction, post_id_s, page_s = call.data.split(":", 4)
    post_id = int(post_id_s)
    page = int(page_s)
    db = session_factory()
    try:
        ok = move_post(db, post_id=post_id, direction=direction)
    finally:
        db.close()
    await call.answer("Готово" if ok else "Нельзя", show_alert=False)
    await _render_list(call, page=page, session_factory=session_factory)


@admin_router.callback_query(F.data.startswith("admin:del:"))
async def admin_delete_post(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, post_id_s, page_s = call.data.split(":", 3)
    post_id = int(post_id_s)
    db = session_factory()
    try:
        ok = delete_post(db, post_id)
    finally:
        db.close()
    await call.answer("Удалено" if ok else "Не найдено")
    await _render_list(call, page=int(page_s), session_factory=session_factory)


@admin_router.callback_query(F.data.startswith("admin:edit:"))
async def admin_open_post(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, post_id_s, page_s = call.data.split(":", 3)
    post_id = int(post_id_s)
    page = int(page_s)
    db = session_factory()
    try:
        post = get_post(db, post_id)
    finally:
        db.close()
    if not post:
        await call.answer("Пост не найден", show_alert=True)
        return
    body = (
        f"<b>День {post.position}. {_h(post.title)}</b>\n"
        f"Медиа: <b>{post.media_type or 'нет'}</b>\n\n"
        f"{post.text_html}"
    )
    await call.message.edit_text(body, reply_markup=admin_edit_post_kb(post_id=post.id, page=page), disable_web_page_preview=True)
    await call.answer()


@admin_router.callback_query(F.data.startswith("admin:edit_title:"))
async def admin_edit_title(call: CallbackQuery, settings: Settings, state: FSMContext):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, post_id_s, page_s = call.data.split(":", 3)
    await state.clear()
    await state.set_state(AdminEditFSM.title)
    await state.update_data(post_id=int(post_id_s), page=int(page_s))
    await call.message.answer("Введите новое <b>название</b> (без «День X.»):")
    await call.answer()


@admin_router.callback_query(F.data.startswith("admin:edit_text:"))
async def admin_edit_text(call: CallbackQuery, settings: Settings, state: FSMContext):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, post_id_s, page_s = call.data.split(":", 3)
    await state.clear()
    await state.set_state(AdminEditFSM.text)
    await state.update_data(post_id=int(post_id_s), page=int(page_s))
    await call.message.answer("Пришлите новый <b>текст</b> (HTML-разметка Telegram допустима):")
    await call.answer()


@admin_router.callback_query(F.data.startswith("admin:edit_media:"))
async def admin_edit_media(call: CallbackQuery, settings: Settings, state: FSMContext):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    _pfx, _cmd, post_id_s, page_s = call.data.split(":", 3)
    await state.clear()
    await state.set_state(AdminEditFSM.media)
    await state.update_data(post_id=int(post_id_s), page=int(page_s))
    await call.message.answer("Пришлите <b>картинку</b> (photo) для поста или текст <code>remove</code>, чтобы убрать картинку:")
    await call.answer()


@admin_router.message(AdminEditFSM.title)
async def admin_save_title(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    title = (message.text or "").strip()
    data = await state.get_data()
    post_id = int(data["post_id"])
    db = session_factory()
    try:
        update_post(db, post_id, title=title)
    finally:
        db.close()
    await state.clear()
    await message.answer("✅ Название обновлено.")


@admin_router.message(AdminEditFSM.text)
async def admin_save_text(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    txt = message.html_text or message.text or ""
    data = await state.get_data()
    post_id = int(data["post_id"])
    db = session_factory()
    try:
        update_post(db, post_id, text_html=txt)
    finally:
        db.close()
    await state.clear()
    await message.answer("✅ Текст обновлён.")


@admin_router.message(AdminEditFSM.media)
async def admin_save_media(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    data = await state.get_data()
    post_id = int(data["post_id"])

    media_type = None
    file_id = None
    if (message.text or "").strip().lower() == "remove":
        media_type = None
        file_id = None
    elif message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    else:
        await message.answer("Нужна картинка (photo) или <code>remove</code>.")
        return

    db = session_factory()
    try:
        update_post(db, post_id, media_type=media_type, file_id=file_id)
    finally:
        db.close()
    await state.clear()
    await message.answer("✅ Медиа обновлено.")


@admin_router.callback_query(F.data == "admin:create")
async def admin_create(call: CallbackQuery, settings: Settings, state: FSMContext):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await state.set_state(AdminEditFSM.create_title)
    await call.message.answer("Введите <b>название</b> нового поста (без «День X.»):")
    await call.answer()


@admin_router.message(AdminEditFSM.create_title)
async def admin_create_title(message: Message, settings: Settings, state: FSMContext):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    title = (message.text or "").strip()
    if not title:
        await message.answer("Название не должно быть пустым.")
        return
    await state.update_data(create_title=title)
    await state.set_state(AdminEditFSM.create_text)
    await message.answer("Пришлите <b>текст</b> нового поста:")


@admin_router.message(AdminEditFSM.create_text)
async def admin_create_text(message: Message, settings: Settings, state: FSMContext):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    txt = message.html_text or message.text or ""
    await state.update_data(create_text=txt)
    await state.set_state(AdminEditFSM.create_media)
    await message.answer("Пришлите <b>картинку</b> (photo) или напишите <code>skip</code>:")


@admin_router.message(AdminEditFSM.create_media)
async def admin_create_media(message: Message, settings: Settings, state: FSMContext, session_factory):
    if not _is_admin(message.from_user.id if message.from_user else None, settings):
        return
    data = await state.get_data()
    title = data["create_title"]
    text_html = data["create_text"]

    media_type = None
    file_id = None
    if (message.text or "").strip().lower() == "skip":
        pass
    elif message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    else:
        await message.answer("Нужна картинка (photo) или <code>skip</code>.")
        return

    db = session_factory()
    try:
        post = create_post(db, title=title, text_html=text_html, media_type=media_type, file_id=file_id)
    finally:
        db.close()

    await state.clear()
    await message.answer(f"✅ Создан пост: День {post.position}. {_h(post.title)}")


@admin_router.callback_query(F.data == "admin:reset:me")
async def admin_reset_me(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    now = _tznow(settings).replace(second=0, microsecond=0)
    db = session_factory()
    try:
        u = db.scalar(select(User).where(User.telegram_id == call.from_user.id))
        if not u:
            await call.answer("Пользователь не найден", show_alert=True)
            return
        delete_task_runs_for_user(db, user_id=u.id)
        reset_progress(db, user_id=u.id, next_send_at=now)
    finally:
        db.close()
    await call.answer("Сброшено ✅", show_alert=True)
    text = await _render_admin_menu_text(telegram_id=call.from_user.id, session_factory=session_factory)
    await call.message.edit_text(text, reply_markup=admins_menu_kb())


@admin_router.callback_query(F.data == "admin:reset:all")
async def admin_reset_all(call: CallbackQuery, settings: Settings, session_factory):
    if not _is_admin(call.from_user.id, settings):
        await call.answer("Нет доступа", show_alert=True)
        return
    now = _tznow(settings).replace(second=0, microsecond=0)
    db = session_factory()
    try:
        users = list(db.scalars(select(User)))
        for u in users:
            delete_task_runs_for_user(db, user_id=u.id)
            reset_progress(db, user_id=u.id, next_send_at=now)
    finally:
        db.close()
    await call.answer("Сброшено для всех ✅", show_alert=True)
    text = await _render_admin_menu_text(telegram_id=call.from_user.id, session_factory=session_factory)
    await call.message.edit_text(text, reply_markup=admins_menu_kb())



