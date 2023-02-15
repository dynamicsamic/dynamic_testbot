from aiogram import Bot, Dispatcher

from . import settings

bot = Bot(token=settings.BOT_TOKEN)
dispatcher = Dispatcher(bot)
