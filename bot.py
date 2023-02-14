from aiogram import Bot, Dispatcher

import settings

bot = Bot(token=settings.BOT_TOKEN)
dp = Dispatcher(bot)
