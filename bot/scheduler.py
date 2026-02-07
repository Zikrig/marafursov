import asyncio
import datetime as dt
import html
import logging
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from bot.config import Settings
from bot.db import Post, Progress, User, TaskRun, count_posts, get_app_settings, get_post_by_position
from bot.keyboards import start_task_kb, summary_kb

logger = logging.getLogger(__name__)
_TICK_LOCK = asyncio.Lock()


def _tznow(settings: Settings) -> dt.datetime:
    # Store/compare all timestamps as tz-naive "local time" in settings.tz
    # to avoid naive/aware comparison issues with SQLite.
    return dt.datetime.now(ZoneInfo(settings.tz)).replace(tzinfo=None)


async def _send_task_notification(bot: Bot, *, chat_id: int, post: Post) -> None:
    safe_title = html.escape(post.title or "")
    text_html = f"Вы получили сегодняшнее задание — <b>{safe_title}</b>\n\nНачать?"
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text_html,
            reply_markup=start_task_kb(post_id=post.id),
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as e:
        # Fallback to plain text if HTML entities/tags break message rendering.
        if "can't parse entities" in str(e).lower():
            text_plain = f"Вы получили сегодняшнее задание — {post.title}\n\nНачать?"
            await bot.send_message(
                chat_id=chat_id,
                text=text_plain,
                reply_markup=start_task_kb(post_id=post.id),
                parse_mode=None,
            )
            return
        raise


async def _send_summary_prompt(bot: Bot, *, chat_id: int) -> None:
    await bot.send_message(
        chat_id=chat_id,
        text="Марафон завершён. Хотите посмотреть свои ответы?",
        reply_markup=summary_kb(),
    )


async def tick(*, bot: Bot, session_factory, settings: Settings) -> None:
    """
    Periodic tick:
    - send tasks when due (daily at configured time)
    - after the last-day window ends (12h after click), show summary button
    - close expired response windows
    """
    async with _TICK_LOCK:
        now = _tznow(settings)

        db: Session = session_factory()
        try:
            # Cross-process safety (important when accidentally running 2 bot instances).
            # If DB is PostgreSQL, use advisory lock so only one scheduler can send at a time.
            lock_key = 917_552_031  # arbitrary constant
            is_pg = bool(getattr(db, "bind", None)) and db.bind.dialect.name == "postgresql"  # type: ignore[attr-defined]
            if is_pg:
                got = db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}).scalar()
                if not got:
                    return
            app = get_app_settings(db)
            interval_min = int(app.send_interval_minutes)
            max_posts = count_posts(db)
            progresses = list(db.scalars(select(Progress).order_by(Progress.next_send_at.asc(), Progress.id.asc())))

            last_post = get_post_by_position(db, position=max_posts) if max_posts else None

            for p in progresses:
                # close expired response window (and update status fields)
                if p.active_until and now >= p.active_until:
                    p.active_post_id = None
                    p.active_started_at = None
                    p.active_until = None
                    p.updated_at = dt.datetime.now()
                    db.commit()

                # last day summary prompt: after last task's response window closes
                # Only if user has actually been sent all tasks (next_position moved past the last one).
                if last_post and not p.summary_prompt_sent and p.next_position > max_posts:
                    last_run = db.scalar(
                        select(TaskRun)
                        .where(TaskRun.user_id == p.user_id, TaskRun.post_id == last_post.id)
                        .order_by(TaskRun.until.desc(), TaskRun.id.desc())
                    )
                    if last_run and now >= last_run.until:
                        chat_id = db.scalar(select(User.telegram_id).where(User.id == p.user_id))
                        if chat_id:
                            try:
                                await _send_summary_prompt(bot, chat_id=int(chat_id))
                                p.summary_prompt_sent = True
                                p.updated_at = dt.datetime.now()
                                db.commit()
                            except Exception:
                                pass

                # due notification (send even if previous wasn't started)
                if p.next_position <= max_posts and p.next_send_at <= now:
                    post = get_post_by_position(db, position=p.next_position)
                    if not post:
                        logger.info(
                            "Skip missing post position=%s user_id=%s (max_posts=%s)",
                            p.next_position,
                            p.user_id,
                            max_posts,
                        )
                        # Do NOT advance next_position; otherwise we "jump" over days.
                        # Just retry later (usually indicates bad JSON seeding / gaps).
                        p.next_send_at = now + dt.timedelta(minutes=interval_min)
                        p.updated_at = dt.datetime.now()
                        db.commit()
                        continue

                    # "close" current active task when a new task arrives (silent)
                    if p.active_post_id is not None:
                        p.active_post_id = None
                        p.active_started_at = None
                        p.active_until = None
                        p.updated_at = dt.datetime.now()
                        db.commit()

                    chat_id = db.scalar(select(User.telegram_id).where(User.id == p.user_id))
                    if not chat_id:
                        logger.warning("No chat_id for user_id=%s, cannot send post_id=%s", p.user_id, post.id)
                        continue
                    try:
                        logger.info(
                            "Sending due task user_id=%s chat_id=%s position=%s post_id=%s (next_send_at=%s now=%s)",
                            p.user_id,
                            chat_id,
                            p.next_position,
                            post.id,
                            p.next_send_at,
                            now,
                        )
                        await _send_task_notification(bot, chat_id=int(chat_id), post=post)
                        p.pending_post_id = post.id
                        p.next_position += 1
                        p.next_send_at = now + dt.timedelta(minutes=interval_min)
                        p.updated_at = dt.datetime.now()
                        db.commit()
                    except Exception:
                        logger.exception(
                            "Failed to send task notification user_id=%s chat_id=%s post_id=%s",
                            p.user_id,
                            chat_id,
                            post.id,
                        )
                        continue
        finally:
            # best-effort unlock
            try:
                is_pg = bool(getattr(db, "bind", None)) and db.bind.dialect.name == "postgresql"  # type: ignore[attr-defined]
                if is_pg:
                    db.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key})
                    db.commit()
            except Exception:
                pass
            db.close()


def setup_scheduler(*, bot: Bot, session_factory, settings: Settings) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=settings.tz)
    scheduler.start()

    async def _tick():
        await tick(bot=bot, session_factory=session_factory, settings=settings)

    scheduler.add_job(
        _tick,
        trigger=IntervalTrigger(seconds=5),
        id="tick",
        replace_existing=True,
        max_instances=1,
    )
    # Fire and forget: run one tick right away. Protected by _TICK_LOCK + PG advisory lock.
    asyncio.get_event_loop().create_task(_tick())
    return scheduler


