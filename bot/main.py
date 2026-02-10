import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot.admin_handlers import admin_router
from bot.config import load_settings
from bot.db import init_db, make_engine, make_session_factory
from bot.handlers import router as user_router
from bot.scheduler import setup_scheduler
from bot.seed_posts import seed_posts_from_json


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # Ensure necessary directories exist
    import os
    os.makedirs("data/images", exist_ok=True)
    os.makedirs("bot_data", exist_ok=True)

    settings = load_settings()
    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)

    if settings.seed_on_start:
        try:
            created = seed_posts_from_json(
                session_factory=session_factory,
                json_path=settings.seed_json_path,
                wipe=settings.seed_wipe_on_start,
            )
            logging.getLogger(__name__).info("Seeded %s posts from %s", created, settings.seed_json_path)
        except Exception:
            logging.getLogger(__name__).exception("Failed to seed posts from %s", settings.seed_json_path)

    bot = Bot(token=settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp["settings"] = settings
    dp["session_factory"] = session_factory

    dp.include_router(user_router)
    dp.include_router(admin_router)

    scheduler = setup_scheduler(bot=bot, session_factory=session_factory, settings=settings)
    dp["scheduler"] = scheduler

    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())


