from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def start_task_kb(*, post_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Начать?", callback_data=f"task:start:{post_id}"))
    return kb.as_markup()


def summary_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Посмотреть мои ответы", callback_data="summary:show"))
    return kb.as_markup()


def summary_full_kb(*, post_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Показать полностью", callback_data=f"summary:full:{post_id}"))
    return kb.as_markup()


def admins_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📋 Посты", callback_data="admin:list:0"))
    kb.row(InlineKeyboardButton(text="✉️ Приветствие", callback_data="admin:greeting"))
    kb.row(InlineKeyboardButton(text="⏱ Окно ответа", callback_data="admin:resp_window"))
    kb.row(InlineKeyboardButton(text="⏲ Интервал рассылки", callback_data="admin:send_interval"))
    kb.row(InlineKeyboardButton(text="📄 Моя сводка", callback_data="admin:summary:me"))
    kb.row(InlineKeyboardButton(text="📊 Сводки всех (Excel)", callback_data="admin:export:xlsx"))
    kb.row(InlineKeyboardButton(text="📣 Рассылка всем", callback_data="admin:broadcast:start"))
    kb.row(InlineKeyboardButton(text="➕ Создать пост", callback_data="admin:create"))
    kb.row(
        InlineKeyboardButton(text="🔄 Сбросить (я)", callback_data="admin:reset:me"),
        InlineKeyboardButton(text="🔄 Сбросить (все)", callback_data="admin:reset:all"),
    )
    return kb.as_markup()


def admin_broadcast_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Отправить всем", callback_data="admin:broadcast:send"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="admin:broadcast:cancel"),
    )
    return kb.as_markup()


def admins_posts_list_kb(*, posts: list[tuple[int, int, str]], page: int, page_size: int, total: int) -> InlineKeyboardMarkup:
    """
    posts: list of (post_id, position, title)
    Row: [Day+Title] [⬆️] [⬇️] [❌]
    """
    kb = InlineKeyboardBuilder()
    for post_id, position, title in posts:
        # Make "Day title" button ~3x larger by putting it alone in a row,
        # and controls in a separate row.
        kb.row(
            InlineKeyboardButton(
                text=f"День {position}. {title}",
                callback_data=f"admin:edit:{post_id}:{page}",
            )
        )
        kb.row(
            InlineKeyboardButton(text="⬆️", callback_data=f"admin:move:up:{post_id}:{page}"),
            InlineKeyboardButton(text="⬇️", callback_data=f"admin:move:down:{post_id}:{page}"),
            InlineKeyboardButton(text="❌", callback_data=f"admin:del:{post_id}:{page}"),
        )

    nav = InlineKeyboardBuilder()
    max_page = max(0, (total - 1) // page_size) if total else 0
    if page > 0:
        nav.add(InlineKeyboardButton(text="⬅️", callback_data=f"admin:list:{page-1}"))
    nav.add(InlineKeyboardButton(text=f"{page+1}/{max_page+1}", callback_data="noop"))
    if page < max_page:
        nav.add(InlineKeyboardButton(text="➡️", callback_data=f"admin:list:{page+1}"))
    kb.row(*nav.buttons)

    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:menu"))
    return kb.as_markup()


def admin_edit_post_kb(*, post_id: int, page: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✏️ Название", callback_data=f"admin:edit_title:{post_id}:{page}"))
    kb.row(InlineKeyboardButton(text="✏️ Текст", callback_data=f"admin:edit_text:{post_id}:{page}"))
    kb.row(InlineKeyboardButton(text="🖼 Картинка", callback_data=f"admin:edit_media:{post_id}:{page}"))
    kb.row(InlineKeyboardButton(text="⬅️ К списку", callback_data=f"admin:list:{page}"))
    return kb.as_markup()


 


