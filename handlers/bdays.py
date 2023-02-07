import yadisk_async
from aiogram import types

import settings
from files import get_file_from_yadisk
from utils import set_inline_button, update_envar

disk = yadisk_async.YaDisk(token=settings.YADISK_TOKEN)


async def cmd_bdays(message: types.Message):
    if not await disk.check_token():
        kbd = set_inline_button(
            text="Получить код", callback_data="confirm_code"
        )
        await message.answer(
            f"Токен безопасности Яндекс Диска устарел.\nДля получения кода подвтерждения нажмите на кнопку ниже и перейдите по ссылке.\nВ открывшейся вкладке браузера войдите в Яндекс аккаунт, на котором хранится Excel файл с данными о днях рождениях. После этого вы автоматически перейдете на страницу получения кода подвтерждения. Скопируйте этот код и отправьте его боту с командой /code.",
            reply_markup=kbd,
        )
    else:
        source_path = settings.YADISK_FILEPATH
        output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
        file = await get_file_from_yadisk(
            message, disk, source_path, output_file.as_posix()
        )
        await message.answer(file)


async def cmd_verify_confirm_code(message: types.Message):
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
                await message.reply(
                    "Код прошел проверку. Получите информацию о днях рождениях партнеров вызвав команду /bdays."
                )
            else:
                await message.answer(
                    "Что-то пошло не так. Обратитесь к разработчику."
                )


async def get_confirm_code(call: types.CallbackQuery):
    disk.id = settings.YANDEX_APP_ID
    disk.secret = settings.YANDEX_SECRET_CLIENT
    url = disk.get_code_url()
    await call.message.answer(url)
    await call.answer()
