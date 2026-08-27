"""Generate a user-account session string (only needed for files > 2 GB).

Log in as the account that will sit in the same group/channel as the bot.
The printed string is a full credential - treat it like a password.
"""
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

import config


async def main():
    async with TelegramClient(StringSession(), config.TG_API_ID, config.TG_API_HASH) as client:
        me = await client.get_me()
        print(f"\nLogged in as {me.first_name} (id {me.id})")
        print("\nUSER_SESSION=" + client.session.save() + "\n")


if __name__ == "__main__":
    asyncio.run(main())
