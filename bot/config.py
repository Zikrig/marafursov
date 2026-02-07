import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _parse_admin_ids(raw: str) -> set[int]:
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(int(part))
    return out


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: set[int]
    tz: str
    database_url: str
    seed_json_path: str
    seed_on_start: bool
    daily_send_hour: int
    daily_send_minute: int
    response_window_hours: int
    max_responses_per_task: int


def load_settings() -> Settings:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set")

    admin_ids = _parse_admin_ids(os.getenv("ADMIN_IDS", "").strip())
    tz = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"
    database_url = os.getenv("DATABASE_URL", "").strip() or "sqlite:///./bot_data/bot.db"

    seed_json_path = os.getenv("SEED_JSON_PATH", "data/challenge_posts.json").strip() or "data/challenge_posts.json"
    seed_on_start = os.getenv("SEED_ON_START", "1").strip().lower() not in ("0", "false", "no")

    daily_send_hour = int(os.getenv("DAILY_SEND_HOUR", "12").strip() or "12")
    daily_send_minute = int(os.getenv("DAILY_SEND_MINUTE", "0").strip() or "0")

    response_window_hours = int(os.getenv("RESPONSE_WINDOW_HOURS", "12").strip() or "12")
    max_responses_per_task = int(os.getenv("MAX_RESPONSES_PER_TASK", "3").strip() or "3")

    return Settings(
        bot_token=bot_token,
        admin_ids=admin_ids,
        tz=tz,
        database_url=database_url,
        seed_json_path=seed_json_path,
        seed_on_start=seed_on_start,
        daily_send_hour=daily_send_hour,
        daily_send_minute=daily_send_minute,
        response_window_hours=response_window_hours,
        max_responses_per_task=max_responses_per_task,
    )


