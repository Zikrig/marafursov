import datetime as dt
import json
import os
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from bot.db import Post


def seed_posts_from_json(*, session_factory, json_path: str, wipe: bool = False) -> int:
    """
    Upsert posts in DB from JSON file (do NOT wipe DB on start).

    JSON format:
    {
      "timezone": "Europe/Moscow",
      "posts": [
        {"day": 1, "title": "...", "text_html": "...", "media_type": "...", "file_id": "..."}
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
        if wipe:
            db.execute(delete(Post))
            db.commit()

        # idempotent upsert: do NOT wipe DB (admin-created posts must survive restarts)
        # NOTE: day numbers are procedural and based on ordering, not JSON "day" field.
        for idx, item in enumerate(posts, start=1):
            title = (item.get("title") or "").strip()
            text_html = item.get("text_html") or ""
            media_type = (item.get("media_type") or "").strip() or None
            file_id = (item.get("file_id") or "").strip() or None
            if not title:
                continue
            existing = db.scalar(select(Post).where(Post.position == idx))
            if existing:
                existing.title = title
                existing.text_html = text_html
                existing.media_type = media_type
                existing.file_id = file_id
                existing.updated_at = dt.datetime.now()
            else:
                p = Post(
                    position=idx,
                    title=title,
                    text_html=text_html,
                    media_type=media_type,
                    file_id=file_id,
                )
                db.add(p)
                created += 1
        db.commit()

        if wipe:
            # If JSON is shorter than previous DB state, ensure tail is removed.
            # (In wipe mode it should already be empty, but keep this safe.)
            db.execute(delete(Post).where(Post.position > len(posts)))
            db.commit()

    finally:
        db.close()
    return created


