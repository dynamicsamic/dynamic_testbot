import logging

import yadisk_async
from aiogram import Bot, types
from aiohttp import ClientSession
from sqlalchemy.exc import IntegrityError

from app import settings
from app.bot import bot
from app.db import Session, models
from app.files import collect_bdays, get_file_from_yadisk
from app.utils import MsgProvider, set_inline_button, update_envar

logger = logging.getLogger(__name__)

disk = yadisk_async.YaDisk(token=settings.YADISK_TOKEN)


async def get_bdays(msg_provider: MsgProvider) -> types.Message:
    """
    Main function for retrieving information about partner's birthdays.
    Uses `msg_provider` to work both with `/` commands sent by users
    via `aiogram.Message` and scheduled jobs sent via `aiogram.Bot` directly.
    Flow:   1.Fetch excel file with birthday data from Yandex.Disk;
            2.Parse excel file into pandas dataframe;
            3.Find today and future birthdays;
            4.Send formatted messages to chat-requester.
    If `Yandex.Disk` token is invalid sends a callback message with button
    for generating another token.
    """
    if not await disk.check_token():
        kbd = set_inline_button(
            text="Получить код", callback_data="confirm_code"
        )
        await msg_provider.dispatch_text(
            "Токен безопасности Яндекс Диска устарел.\n"
            "Для получения кода подвтерждения нажмите на кнопку ниже и "
            "перейдите по ссылке.\nВ открывшейся вкладке браузера войдите в "
            "Яндекс аккаунт, на котором хранится Excel файл с данными о днях "
            "рождениях. После этого вы автоматически перейдете на страницу "
            "получения кода подвтерждения. Скопируйте этот код и отправьте его "
            "боту с командой /code.",
            reply_markup=kbd,
        )
    else:
        source_path = settings.YADISK_FILEPATH
        output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
        file = await get_file_from_yadisk(
            msg_provider, disk, source_path, output_file.as_posix()
        )
        if file:
            async with ClientSession() as session:
                await collect_bdays(
                    msg_provider, session, settings.OUTPUT_FILE_NAME
                )


async def get_bdays_job(bot: Bot, chat_id: int):
    """
    Send message with partners' birthdays info
    to selected telegram chat directly via aiogram.Bot.
    Intended to be used in scheduled jobs.
    """
    await get_bdays(MsgProvider(bot, chat_id=chat_id))


async def cmd_add_bdays_job(message: types.Message):
    """
    Command for adding `get_birthday` job to scheduler for
    the chat-requester.
    Currently days and time are unavailable to choose.
    Need to implement tihs later.
    """
    chat_id = message.chat.id
    with Session() as session:
        if chat_id not in models.TelegramChat.ids(session, to_list=True):
            session.add(models.TelegramChat(tg_chat_id=chat_id))
            session.commit()
            await message.answer(
                "Ежедневная рассылка списка дней рождения партнеров "
                "для данного чата запланирована.\n"
                "Рассылка осуществляется каждый день в 09:00 МСК."
            )
            logger.info(f"Chat[{chat_id}] added to mailing list")
        else:
            await message.answer(
                "Данный чат уже получает ежедневную рассылку "
                "дней рождения партнеров."
            )
            logger.info(
                f"Chat duplication attempt error; skipped for chat {chat_id}"
            )


async def cmd_bdays(message: types.Message):
    """
    Command for sending message with partners' birthdays info
    to chat-requester via aiogram.Message.
    Manual per-request counterpart of scheduled `get_bdays_job`.
    """
    await get_bdays(MsgProvider(message))


async def cmd_verify_confirm_code(message: types.Message):
    """
    Command for Yandex.Disk confirmation code verification.
    If code valid, sets new token to yadisk_async.YaDisk instance.
    If code is invalid sends a callback message with button
    for generating new code.
    """
    confrm_code = message.get_args()
    if not confrm_code:
        await message.reply(
            "Вы не передали код. Попробуйте еще раз.\n"
            "Введите команду /code, добавьте пробел и напишите ваш код."
        )
    else:
        try:
            resp = await disk.get_token(confrm_code)
        except yadisk_async.exceptions.BadRequestError:
            kbd = set_inline_button(
                text="Получить ссылку на новый код",
                callback_data="confirm_code",
            )
            await message.answer(
                "Вы ввели неверный код. Попробуйте получить новый код.",
                reply_markup=kbd,
            )
        else:
            new_token = resp.access_token
            disk.token = new_token
            if await disk.check_token():
                # update YADISK_TOKEN env var
                update_envar(
                    settings.BASE_DIR / ".env", "YADISK_TOKEN_TEST", new_token
                )
                await message.answer(
                    "Код прошел проверку. Получите информацию о днях рождениях "
                    "партнеров вызвав команду /bdays."
                )
            else:
                await message.answer(
                    "Что-то пошло не так. Обратитесь к разработчику."
                )


async def get_confirm_code(call: types.CallbackQuery):
    """Callback for sending 'obtain Yandex.Disk code' url to user."""
    disk.id = settings.YANDEX_APP_ID
    disk.secret = settings.YANDEX_SECRET_CLIENT
    url = disk.get_code_url()
    await call.message.answer(url)
    await call.answer()
