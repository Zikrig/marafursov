import datetime as dt
import json
import os
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from bot.db import Post


def seed_posts_from_json(*, session_factory, json_path: str) -> int:
    """
    Replace posts in DB from JSON file.

    JSON format:
    {
      "timezone": "Europe/Moscow",
      "posts": [
        {"day": 1, "title": "...", "text_html": "..."}
      ]
    }
    """
    path = Path(json_path)
    if not path.is_absolute():
        path = Path(os.getcwd()) / path
    raw = json.loads(path.read_text(encoding="utf-8"))
    posts = raw.get("posts", [])

    db: Session = session_factory()
    created = 0
    try:
        # idempotent upsert: do NOT wipe DB (admin-created posts must survive restarts)
        for item in posts:
            day = int(item.get("day") or 0)
            title = (item.get("title") or "").strip()
            text_html = item.get("text_html") or ""
            if not day or not title:
                continue
            existing = db.scalar(select(Post).where(Post.position == day))
            if existing:
                existing.title = title
                existing.text_html = text_html
                existing.updated_at = dt.datetime.now()
            else:
                p = Post(
                    position=day,
                    title=title,
                    text_html=text_html,
                    media_type=None,
                    file_id=None,
                )
                db.add(p)
                created += 1
        db.commit()
    finally:
        db.close()
    return created


