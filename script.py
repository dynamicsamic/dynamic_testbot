import asyncio
import logging
from logging.config import fileConfig

from decouple import config
from telegram import (
    Bot,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

fileConfig(fname="log_config.conf", disable_existing_loggers=False)

logger = logging.getLogger(__name__)

BOT_TOKEN = config("BOT_TOKEN")

# bot = Bot(BOT_TOKEN)
# bot.send_message()


async def start(update: Update, context):
    await context.bot.send_message(
        chat_id=update.effective_chat.id, text="press start to start"
    )


async def menu(update: Update, context):
    print(await context.bot.get_my_commands())
    await context.bot.send_message(
        chat_id=update.effective_chat.id, text="look at the menu"
    )


async def echo(update: Update, context):
    await context.bot.send_message(
        chat_id=update.effective_chat.id, text=update.message.text
    )


async def unknown_command(update: Update, context):
    commands = await context.bot.get_my_commands()
    await context.bot.send_message(
        chat_id=update.effective_chat.id, text="Неизвестная команда"
    )


async def insult(update: Update, context):
    query = update.inline_query.query
    logger.info(f"{query}")
    if not query:
        return
    results = []
    results.append(
        InlineQueryResultArticle(
            id="You suck!",
            title="Insult",
            input_message_content=InputTextMessageContent("You suck!"),
        )
    )
    await context.bot.answer_inline_query(update.inline_query.id, results)


import json

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    start_handler = CommandHandler("start", start)
    menu_handler = CommandHandler("menu", menu)
    unknown_command_handler = MessageHandler(filters.COMMAND, unknown_command)
    echo_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), echo)
    inline_insult_handler = InlineQueryHandler(insult)
    app.add_handlers(
        [
            start_handler,
            menu_handler,
            inline_insult_handler,
            echo_handler,
            unknown_command_handler,
        ]
    )
    app.bot.set_my_commands(
        {
            "commands": json.dumps(
                [
                    {"command": "start", "description": "Start using bot"},
                    {"command": "help", "description": "Display help"},
                    {"command": "menu", "description": "Display menu"},
                ]
            )
        }
    )
    app.run_polling()
