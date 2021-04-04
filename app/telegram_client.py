import os
from telethon import TelegramClient
from telethon.sessions import MemorySession


class Telegram:
    """
    Telegram client to send data
    """

    def __init__(self):
        api_id = int(os.getenv("TELEGRAM_API_ID"))
        api_hash = os.getenv("TELEGRAM_API_HASH")
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.client = TelegramClient(MemorySession(), api_id, api_hash)
        self.client.start(bot_token=bot_token)
