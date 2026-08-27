"""Telegram -> Google Drive bridge (MTProto, handles files up to 4 GB)."""
import asyncio
import logging
import mimetypes
import os
import re
import shutil
import time

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.sessions import StringSession

import config
import drive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
log = logging.getLogger("bot")

UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
EDIT_EVERY = 4.0  # seconds between status-message edits

bot = TelegramClient("bot", config.TG_API_ID, config.TG_API_HASH)
user = (
    TelegramClient(StringSession(config.USER_SESSION), config.TG_API_ID, config.TG_API_HASH)
    if config.USER_SESSION
    else None
)
queue = asyncio.Queue()


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{int(n)}B" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def safe_name(name, fallback):
    name = UNSAFE.sub("_", (name or "").strip()) or fallback
    return name[:200]


class Progress:
    """Shared counter written by download/upload, read by the status ticker."""

    def __init__(self):
        self.stage = "starting"
        self.done = 0
        self.total = 0
        self.started = time.monotonic()

    def begin(self, stage, total):
        self.stage, self.done, self.total = stage, 0, total
        self.started = time.monotonic()

    def update(self, done, total=None):
        self.done = done
        if total:
            self.total = total

    def render(self, title):
        total, done = self.total, self.done
        pct = (done / total * 100) if total else 0
        filled = int(pct // 5)
        elapsed = max(time.monotonic() - self.started, 0.001)
        speed = done / elapsed
        eta = (total - done) / speed if speed > 0 and total else 0
        return (
            f"**{title}**\n"
            f"`{'#' * filled}{'.' * (20 - filled)}` {pct:5.1f}%\n"
            f"{self.stage}: {human(done)} / {human(total)}\n"
            f"{human(speed)}/s - ETA {int(eta // 60)}m{int(eta % 60):02d}s"
        )


async def ticker(status_msg, prog, title, stop):
    """Edit the status message on an interval until `stop` is set."""
    last = ""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=EDIT_EVERY)
            return
        except asyncio.TimeoutError:
            pass
        text = prog.render(title)
        if text == last:
            continue
        try:
            await status_msg.edit(text)
            last = text
        except (MessageNotModifiedError, FloodWaitError):
            pass
        except Exception as exc:  # a failed status edit must not kill the transfer
            log.debug("status edit failed: %s", exc)


async def resolve_source(chat_id, msg_id, size):
    """Pick the client that is actually able to download a file of this size."""
    if size <= config.BOT_DOWNLOAD_LIMIT:
        return await bot.get_messages(chat_id, ids=msg_id), "bot"

    if user is None:
        raise RuntimeError(
            f"This file is {human(size)}. A bot account cannot download past "
            f"{human(config.BOT_DOWNLOAD_LIMIT)}. Set USER_SESSION in .env "
            "(see `python gen_session.py`) to enable large transfers."
        )

    try:
        msg = await user.get_messages(chat_id, ids=msg_id)
    except Exception as exc:
        raise RuntimeError(
            "The user account could not read this chat. For files over "
            f"{human(config.BOT_DOWNLOAD_LIMIT)}, post the file in a group or "
            "channel that BOTH the bot and the user account belong to.\n"
            f"({type(exc).__name__}: {exc})"
        ) from exc

    if msg is None or msg.file is None:
        raise RuntimeError(
            "The user account is not a member of this chat, so it cannot fetch "
            "the file. Move the upload to a shared group or channel."
        )
    return msg, "user"


async def handle_job(chat_id, msg_id, reply_to):
    status = await bot.send_message(chat_id, "Queued...", reply_to=reply_to)
    path = None
    try:
        probe = await bot.get_messages(chat_id, ids=msg_id)
        if probe is None or probe.file is None:
            await status.edit("That message no longer has a file attached.")
            return

        size = probe.file.size or 0
        name = safe_name(probe.file.name, f"telegram_{msg_id}{probe.file.ext or ''}")
        mime = probe.file.mime_type or mimetypes.guess_type(name)[0]

        free = shutil.disk_usage(config.DOWNLOAD_DIR).free
        if free < size * 1.05:
            await status.edit(
                f"Not enough disk space: need ~{human(size * 1.05)}, "
                f"{human(free)} free in {config.DOWNLOAD_DIR}."
            )
            return

        message, via = await resolve_source(chat_id, msg_id, size)
        client = bot if via == "bot" else user
        path = os.path.join(config.DOWNLOAD_DIR, f"{msg_id}_{name}")

        prog = Progress()
        stop = asyncio.Event()
        tick = asyncio.create_task(ticker(status, prog, name, stop))
        try:
            prog.begin(f"downloading (via {via})", size)
            await client.download_media(message, file=path, progress_callback=prog.update)

            prog.begin("uploading to Drive", os.path.getsize(path))
            loop = asyncio.get_running_loop()

            def on_chunk(done, total):
                loop.call_soon_threadsafe(prog.update, done, total)

            meta = await asyncio.to_thread(drive.upload, path, name, mime, on_chunk)
        finally:
            stop.set()
            await tick

        link = meta.get("webViewLink") or f"https://drive.google.com/file/d/{meta['id']}/view"
        await status.edit(
            f"**Uploaded**\n`{name}`\n{human(size)}\n\n{link}", link_preview=False
        )
        log.info("uploaded %s (%s) as %s", name, human(size), meta["id"])

    except Exception as exc:
        log.exception("job failed")
        try:
            await status.edit(f"**Failed**\n`{type(exc).__name__}`\n{exc}")
        except Exception:
            pass
    finally:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError as exc:
                log.warning("could not remove %s: %s", path, exc)


async def worker():
    """One transfer at a time - bounds disk, RAM and bandwidth."""
    while True:
        job = await queue.get()
        try:
            await handle_job(*job)
        finally:
            queue.task_done()


@bot.on(events.NewMessage(pattern=r"^/(start|help)"))
async def on_start(event):
    await event.reply(
        "Send me a file and I'll put it in Google Drive.\n\n"
        f"- up to {human(config.BOT_DOWNLOAD_LIMIT)}: send it here directly\n"
        "- larger: post it in a group I share with the linked user account\n\n"
        "/id - show your Telegram user id"
    )


@bot.on(events.NewMessage(pattern=r"^/id"))
async def on_id(event):
    await event.reply(f"Your user id: `{event.sender_id}`\nChat id: `{event.chat_id}`")


@bot.on(events.NewMessage(func=lambda e: e.file is not None))
async def on_file(event):
    if event.sender_id not in config.ALLOWED_USERS:
        log.warning("rejected file from %s", event.sender_id)
        log.warning(
            "rejected file from sender_id=%r (type %s); ALLOWED_USERS=%r",
            event.sender_id, type(event.sender_id).__name__, config.ALLOWED_USERS,
        )
        await event.reply(
            f"Not authorised. Your id is `{event.sender_id}` - "
            "add it to ALLOWED_USERS and restart the bot."
        )
        return
    await queue.put((event.chat_id, event.id, event.id))
    if queue.qsize() > 1:
        await event.reply(f"Queued - {queue.qsize()} ahead of this one.")


async def main():
    log.info("ALLOWED_USERS loaded as: %r", config.ALLOWED_USERS)
    if not config.ALLOWED_USERS:
        log.warning("ALLOWED_USERS is empty - every upload will be rejected.")

    drive.load_credentials()  # fail fast if Drive auth is not set up
    log.info("Drive credentials OK")

    await bot.start(bot_token=config.BOT_TOKEN)
    me = await bot.get_me()
    log.info("bot @%s ready", me.username)

    if user:
        await user.start()
        who = await user.get_me()
        log.info("user session active: %s (id %s)", who.first_name, who.id)
    else:
        log.info(
            "no USER_SESSION - files over %s will be refused",
            human(config.BOT_DOWNLOAD_LIMIT),
        )

    asyncio.create_task(worker())
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
