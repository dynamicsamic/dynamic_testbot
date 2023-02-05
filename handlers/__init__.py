from aiogram import Dispatcher

from .common import cmd_help


def register_handlers_common(dp: Dispatcher):
    dp.register_message_handler(cmd_help, commands=["help"])
