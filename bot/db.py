import datetime as dt
import os
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False)


class Post(Base):
    """
    Task/post template.

    Day number is procedural and equals `position` in the current ordering.
    """

    __tablename__ = "posts"
    __table_args__ = (UniqueConstraint("position", name="uq_posts_position"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False, index=True)  # 1..N
    title: Mapped[str] = mapped_column(String(120), default="", nullable=False)
    text_html: Mapped[str] = mapped_column(Text, default="", nullable=False)
    media_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    file_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False)


class Progress(Base):
    """
    Per-user progress.

    - next_position: which day/task to notify next
    - pending_post_id: last notified task awaiting "Начать?" click
    - active_post_id + active_until: response window after click
    """

    __tablename__ = "progress"
    __table_args__ = (UniqueConstraint("user_id", name="uq_progress_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)

    next_position: Mapped[int] = mapped_column(Integer, default=1, nullable=False)  # next to notify
    next_send_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False, index=True)

    pending_post_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("posts.id", ondelete="SET NULL"), nullable=True, index=True)

    active_post_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("posts.id", ondelete="SET NULL"), nullable=True, index=True)
    active_started_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime(), nullable=True)
    active_until: Mapped[Optional[dt.datetime]] = mapped_column(DateTime(), nullable=True, index=True)
    summary_prompt_sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False)


class Response(Base):
    __tablename__ = "responses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("task_runs.id", ondelete="CASCADE"), index=True, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    post_id: Mapped[int] = mapped_column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), index=True, nullable=False)
    seq: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False, index=True)


class TaskRun(Base):
    """
    User clicked "Начать?" for a post -> opens a response window until `until`.
    Multiple runs can coexist if the admin configures a send interval smaller than the response window.
    """

    __tablename__ = "task_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    post_id: Mapped[int] = mapped_column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), index=True, nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(), nullable=False, index=True)
    until: Mapped[dt.datetime] = mapped_column(DateTime(), nullable=False, index=True)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False)


class AppSettings(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    greeting_text: Mapped[str] = mapped_column(
        Text,
        default="Добро пожаловать в марафон!\n\nСкоро пришлю первое задание.",
        nullable=False,
    )
    response_window_minutes: Mapped[int] = mapped_column(Integer, default=12 * 60, nullable=False)
    send_interval_minutes: Mapped[int] = mapped_column(Integer, default=1440, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(), default=lambda: dt.datetime.now(), nullable=False)


def make_engine(database_url: str):
    # Ensure local folder exists for sqlite relative path
    if database_url.startswith("sqlite:///./"):
        os.makedirs("bot_data", exist_ok=True)
    
    # Настройки пула соединений для PostgreSQL
    pool_kwargs = {}
    if database_url.startswith("postgres"):
        pool_kwargs = {
            "pool_size": 10,  # Размер пула соединений
            "max_overflow": 20,  # Максимальное количество дополнительных соединений
            "pool_timeout": 30,  # Таймаут ожидания соединения
            "pool_recycle": 3600,  # Переиспользование соединений через час
            "pool_pre_ping": True,  # Проверка соединений перед использованием
        }
    
    return create_engine(database_url, future=True, **pool_kwargs)


def make_session_factory(engine):
    return sessionmaker(bind=engine, expire_on_commit=False, class_=Session, future=True)


def init_db(engine) -> None:
    os.makedirs("bot_data", exist_ok=True)

    # SQLite: best-effort schema compatibility for rapid iteration.
    # If the existing DB has an old schema (missing columns), drop and recreate our tables.
    try:
        if str(engine.url).startswith("sqlite"):
            with engine.connect() as conn:
                def table_columns(table: str) -> set[str]:
                    rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
                    return {r[1] for r in rows}  # (cid, name, type, notnull, dflt_value, pk)

                expected_users = {"id", "telegram_id", "is_admin", "created_at"}
                expected_posts = {"id", "position", "title", "text_html", "media_type", "file_id", "updated_at"}
                expected_progress = {
                    "id",
                    "user_id",
                    "next_position",
                    "next_send_at",
                    "pending_post_id",
                    "active_post_id",
                    "active_started_at",
                    "active_until",
                    "summary_prompt_sent",
                    "updated_at",
                }
                expected_task_runs = {"id", "user_id", "post_id", "started_at", "until", "updated_at"}
                expected_responses = {"id", "run_id", "user_id", "post_id", "seq", "text", "created_at"}
                expected_app_settings = {"id", "greeting_text", "response_window_minutes", "send_interval_minutes", "updated_at"}

                users_cols = table_columns("users")
                posts_cols = table_columns("posts")
                progress_cols = table_columns("progress")
                task_runs_cols = table_columns("task_runs")
                responses_cols = table_columns("responses")
                app_settings_cols = table_columns("app_settings")

                # Drop in reverse dependency order
                if responses_cols and responses_cols != expected_responses:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS responses")
                if task_runs_cols and task_runs_cols != expected_task_runs:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS task_runs")
                if progress_cols and progress_cols != expected_progress:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS progress")
                if posts_cols and posts_cols != expected_posts:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS posts")
                if users_cols and users_cols != expected_users:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS users")

                if app_settings_cols and app_settings_cols != expected_app_settings:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS app_settings")

                conn.commit()
    except Exception:
        # Don't block startup on schema checks (e.g. non-sqlite, permission issues).
        pass

    Base.metadata.create_all(bind=engine)


def get_app_settings(db: Session) -> AppSettings:
    s = db.get(AppSettings, 1)
    if s:
        return s
    s = AppSettings(id=1)
    db.add(s)
    db.commit()
    return s


def set_greeting_text(db: Session, *, text: str) -> AppSettings:
    s = get_app_settings(db)
    s.greeting_text = text
    s.updated_at = dt.datetime.now()
    db.commit()
    return s


def set_response_window_minutes(db: Session, *, minutes: int) -> AppSettings:
    s = get_app_settings(db)
    m = int(minutes)
    if m < 1:
        m = 1
    if m > 60 * 24 * 7:
        m = 60 * 24 * 7
    s.response_window_minutes = m
    s.updated_at = dt.datetime.now()
    db.commit()
    return s


def set_send_interval_minutes(db: Session, *, minutes: int) -> AppSettings:
    s = get_app_settings(db)
    m = int(minutes)
    if m < 1:
        m = 1
    if m > 60 * 24 * 365:
        m = 60 * 24 * 365
    s.send_interval_minutes = m
    s.updated_at = dt.datetime.now()
    db.commit()
    return s


def delete_user_by_telegram_id(db: Session, telegram_id: int) -> bool:
    u = db.scalar(select(User).where(User.telegram_id == telegram_id))
    if not u:
        return False
    db.delete(u)
    db.commit()
    return True


def delete_task_runs_for_user(db: Session, *, user_id: int) -> None:
    """
    Remove all task runs and responses for a user.
    Used by admin reset to avoid "marathon finished" prompt from old last-day runs.
    """
    # Responses may not cascade in SQLite depending on FK settings, so delete explicitly.
    db.execute(delete(Response).where(Response.user_id == user_id))
    db.execute(delete(TaskRun).where(TaskRun.user_id == user_id))
    db.commit()


def now_utc() -> dt.datetime:
    return dt.datetime.utcnow()


def upsert_user(db: Session, telegram_id: int) -> User:
    user = db.scalar(select(User).where(User.telegram_id == telegram_id))
    if user:
        return user
    user = User(telegram_id=telegram_id, is_admin=False)
    db.add(user)
    db.commit()
    return user


def set_user_admin_flag(db: Session, telegram_id: int, is_admin: bool) -> None:
    db.execute(update(User).where(User.telegram_id == telegram_id).values(is_admin=is_admin))
    db.commit()


def get_user_by_telegram_id(db: Session, telegram_id: int) -> Optional[User]:
    """
    Получить пользователя по telegram_id.
    """
    return db.scalar(select(User).where(User.telegram_id == telegram_id))


def get_or_create_progress(db: Session, *, user_id: int, next_send_at: dt.datetime) -> Progress:
    p = db.scalar(select(Progress).where(Progress.user_id == user_id))
    if p:
        return p
    p = Progress(user_id=user_id, next_position=1, next_send_at=next_send_at)
    db.add(p)
    db.commit()
    return p


def reset_progress(db: Session, *, user_id: int, next_send_at: dt.datetime) -> None:
    p = db.scalar(select(Progress).where(Progress.user_id == user_id))
    if not p:
        p = Progress(user_id=user_id, next_position=1, next_send_at=next_send_at)
        db.add(p)
    else:
        p.next_position = 1
        p.next_send_at = next_send_at
        p.pending_post_id = None
        p.active_post_id = None
        p.active_started_at = None
        p.active_until = None
        p.summary_prompt_sent = False
        p.updated_at = dt.datetime.now()
    db.commit()


def get_all_users(db: Session) -> list[User]:
    return list(db.scalars(select(User)))

def count_users(db: Session) -> int:
    return int(db.scalar(select(func.count()).select_from(User)) or 0)


def count_posts(db: Session) -> int:
    stmt = select(func.count()).select_from(Post)
    return int(db.scalar(stmt) or 0)


def list_posts(db: Session, *, limit: int, offset: int) -> list[Post]:
    stmt = (
        select(Post)
        .order_by(Post.position.asc(), Post.id.asc())
        .limit(limit)
        .offset(offset)
    )
    return list(db.scalars(stmt))


def get_post_by_position(db: Session, *, position: int) -> Optional[Post]:
    return db.scalar(select(Post).where(Post.position == position))


def get_post(db: Session, post_id: int) -> Optional[Post]:
    return db.scalar(select(Post).where(Post.id == post_id))


def create_post(db: Session, *, title: str, text_html: str, media_type: Optional[str], file_id: Optional[str]) -> Post:
    max_pos = int(db.scalar(select(func.max(Post.position))) or 0)
    post = Post(
        position=max_pos + 1,
        title=title.strip(),
        text_html=text_html,
        media_type=media_type,
        file_id=file_id,
        updated_at=dt.datetime.now(),
    )
    db.add(post)
    db.commit()
    return post


def update_post(db: Session, post_id: int, *, title: Optional[str] = None, text_html: Optional[str] = None, media_type: Optional[str] = None, file_id: Optional[str] = None) -> Optional[Post]:
    post = get_post(db, post_id)
    if not post:
        return None
    if title is not None:
        post.title = title.strip()
    if text_html is not None:
        post.text_html = text_html
    if media_type is not None or file_id is not None:
        post.media_type = media_type
        post.file_id = file_id
    post.updated_at = dt.datetime.now()
    db.commit()
    return post


def delete_post(db: Session, post_id: int) -> bool:
    post = get_post(db, post_id)
    if not post:
        return False
    pos = post.position
    db.delete(post)
    db.commit()
    # shift down
    db.execute(
        update(Post)
        .where(Post.position > pos)
        .values(position=Post.position - 1, updated_at=dt.datetime.now())
    )
    db.commit()
    return True


def move_post(db: Session, *, post_id: int, direction: str) -> bool:
    post = get_post(db, post_id)
    if not post:
        return False
    if direction not in ("up", "down"):
        return False
    delta = -1 if direction == "up" else 1
    target_pos = post.position + delta
    if target_pos < 1:
        return False
    other = db.scalar(select(Post).where(Post.position == target_pos))
    if not other:
        return False
    # swap positions safely under UNIQUE(level, position) (SQLite checks immediately)
    now = dt.datetime.now()
    src_pos = post.position
    dst_pos = other.position

    # Use a temporary position that can't collide (0 is outside 1..N).
    post.position = 0
    post.updated_at = now
    db.flush()

    other.position = src_pos
    other.updated_at = now
    db.flush()

    post.position = dst_pos
    post.updated_at = now
    db.flush()

    db.commit()
    return True


def create_task_run(db: Session, *, user_id: int, post_id: int, started_at: dt.datetime, until: dt.datetime) -> TaskRun:
    run = TaskRun(user_id=user_id, post_id=post_id, started_at=started_at, until=until, updated_at=dt.datetime.now())
    db.add(run)
    db.commit()
    return run


def get_latest_open_run(db: Session, *, user_id: int, now: dt.datetime) -> Optional[TaskRun]:
    return db.scalar(
        select(TaskRun)
        .where(TaskRun.user_id == user_id, TaskRun.until >= now)
        .order_by(TaskRun.started_at.desc(), TaskRun.id.desc())
    )


def get_latest_open_run_for_post(db: Session, *, user_id: int, post_id: int, now: dt.datetime) -> Optional[TaskRun]:
    return db.scalar(
        select(TaskRun)
        .where(TaskRun.user_id == user_id, TaskRun.post_id == post_id, TaskRun.until >= now)
        .order_by(TaskRun.started_at.desc(), TaskRun.id.desc())
    )


def add_response(db: Session, *, run_id: int, user_id: int, post_id: int, text: str) -> Response:
    seq = int(db.scalar(select(func.max(Response.seq)).where(Response.run_id == run_id)) or 0) + 1
    r = Response(run_id=run_id, user_id=user_id, post_id=post_id, seq=seq, text=text)
    db.add(r)
    db.commit()
    return r


def count_responses_for_run(db: Session, *, run_id: int) -> int:
    return int(db.scalar(select(func.count()).select_from(Response).where(Response.run_id == run_id)) or 0)


def close_run_now(db: Session, *, run_id: int, now: dt.datetime) -> None:
    # Ensure it is considered closed for any subsequent "now" comparisons.
    closed_until = now - dt.timedelta(microseconds=1)
    db.execute(update(TaskRun).where(TaskRun.id == run_id).values(until=closed_until, updated_at=dt.datetime.now()))
    db.commit()


def get_responses_for_user(db: Session, *, user_id: int) -> list[tuple[Post, list[Response]]]:
    """
    Returns summary items per post. If a post has multiple TaskRuns for the user,
    we include responses only from the latest run (by started_at).
    """
    posts = list(db.scalars(select(Post).order_by(Post.position.asc(), Post.id.asc())))
    if not posts:
        return []

    post_ids = [p.id for p in posts]

    # latest run per post for this user
    latest_runs = list(
        db.execute(
            select(
                TaskRun.post_id,
                func.max(TaskRun.started_at).label("max_started_at"),
            )
            .where(TaskRun.user_id == user_id, TaskRun.post_id.in_(post_ids))
            .group_by(TaskRun.post_id)
        ).all()
    )
    if not latest_runs:
        return [(p, []) for p in posts]

    # Map post_id -> latest started_at
    latest_started_at: dict[int, dt.datetime] = {int(pid): msa for (pid, msa) in latest_runs if msa is not None}

    # Fetch run ids that match (post_id, started_at)
    run_rows = list(
        db.execute(
            select(TaskRun.id, TaskRun.post_id)
            .where(
                TaskRun.user_id == user_id,
                TaskRun.post_id.in_(list(latest_started_at.keys())),
                TaskRun.started_at.in_(list(latest_started_at.values())),
            )
        ).all()
    )
    run_by_post: dict[int, int] = {int(post_id): int(run_id) for (run_id, post_id) in run_rows}

    run_ids = list(run_by_post.values())
    rs = list(
        db.scalars(
            select(Response)
            .where(Response.user_id == user_id, Response.run_id.in_(run_ids))
            .order_by(Response.post_id.asc(), Response.seq.asc(), Response.id.asc())
        )
    )
    by_post: dict[int, list[Response]] = {}
    for r in rs:
        by_post.setdefault(r.post_id, []).append(r)

    return [(p, by_post.get(p.id, [])) for p in posts]
