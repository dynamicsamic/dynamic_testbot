from aiogram import Dispatcher

from .bdays import (
    cmd_add_chat_to_bdays_mailing,
    cmd_bdays,
    cmd_verify_confirm_code,
    get_confirm_code,
)
from .common import cmd_start, cmd_test_thing


def register_common_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_start, commands=["start"])
    # dp.register_message_handler(cmd_help, commands=["help"])
    dp.register_message_handler(cmd_test_thing, commands=["test"])


def register_bdays_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_bdays, commands=["bdays"])
    dp.register_message_handler(cmd_verify_confirm_code, commands=["code"])
    dp.register_callback_query_handler(get_confirm_code, text="confirm_code")
    dp.register_message_handler(
        cmd_add_chat_to_bdays_mailing, commands=["addchat"]
    )
