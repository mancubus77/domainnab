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
        self.client = TelegramClient(MemorySession(), api_id, api_hash).start(
            bot_token=bot_token
        )

    def send_telegram_message(self, receiver, message) -> None:
        """
        Sends message in async mode
        @param receiver: username and user id
        @param message: message to send
        @return: None
        """
        with self.client:
            self.client.loop.run_until_complete(
                self.client.send_message(receiver, message)
            )
