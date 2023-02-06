from aiogram import types

START_MESSAGE = (
    "Привет :wave:"
    "\n"
    "Я бот помощник\."
    "\n"
    "Чтобы посмотреть список моих команд, введите символ /"
)


async def cmd_start(message: types.Message):
    await message.answer(
        text=START_MESSAGE, parse_mode=types.ParseMode.MARKDOWN_V2
    )


async def cmd_help(message: types.Message):
    await message.answer("WElcome my friend!")


async def cmd_test_thing(message: types.Message):
    pass


# @dp.message_handler(commands=["joke"])
# async def send_joke():
#    pass
