"""Telegram -> Google Drive bridge (MTProto, handles files up to 4 GB, Torrents/Magnets & Cancellation)."""
import asyncio
import logging
import mimetypes
import os
import re
import shutil
import threading
import time
import uuid
from typing import Any, Dict, Optional

from telethon import Button, TelegramClient, events
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.sessions import StringSession

import config
import drive
from drive import TransferCancelled
from torrent import Aria2NotInstalledError, torrent_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
log = logging.getLogger("bot")

UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
EDIT_EVERY = 4.0  # seconds between status-message edits
MAGNET_REGEX = re.compile(r"(?i)^(?:/(?:magnet|mirror)\s+)?(magnet:\?xt=urn:[^\s]+)")

bot = TelegramClient("bot", config.TG_API_ID, config.TG_API_HASH)
user = (
    TelegramClient(StringSession(config.USER_SESSION), config.TG_API_ID, config.TG_API_HASH)
    if config.USER_SESSION
    else None
)
queue = asyncio.Queue()


def human(n):
    if n is None:
        return "0B"
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
        self.speed = 0
        self.seeds = None
        self.peers = None
        self.started = time.monotonic()

    def begin(self, stage, total=0, speed=0, seeds=None, peers=None):
        self.stage = stage
        self.done = 0
        self.total = total
        self.speed = speed
        self.seeds = seeds
        self.peers = peers
        self.started = time.monotonic()

    def update(self, done, total=None, speed=0, seeds=None, peers=None):
        self.done = done
        if total is not None:
            self.total = total
        if speed > 0:
            self.speed = speed
        if seeds is not None:
            self.seeds = seeds
        if peers is not None:
            self.peers = peers

    def update_torrent(self, d: dict):
        self.stage = d.get("stage", "downloading (torrent)")
        self.done = d.get("done", 0)
        self.total = d.get("total", 0)
        self.speed = d.get("speed", 0)
        self.seeds = d.get("seeds")
        self.peers = d.get("peers")

    def render(self, title):
        total, done = self.total, self.done
        pct = (done / total * 100) if total else 0
        filled = min(max(int(pct // 5), 0), 20)
        elapsed = max(time.monotonic() - self.started, 0.001)
        calc_speed = self.speed if self.speed > 0 else (done / elapsed)
        eta = (total - done) / calc_speed if calc_speed > 0 and total else 0

        lines = [
            f"**{title}**",
            f"`[{'#' * filled}{'.' * (20 - filled)}]` {pct:5.1f}%",
        ]
        if total > 0:
            lines.append(f"{self.stage}: {human(done)} / {human(total)}")
            lines.append(f"⚡ {human(calc_speed)}/s - ⏳ ETA {int(eta // 60)}m{int(eta % 60):02d}s")
        else:
            lines.append(f"{self.stage}: {human(done)}")
            lines.append(f"⚡ {human(calc_speed)}/s")

        if self.seeds is not None or self.peers is not None:
            s = self.seeds if self.seeds is not None else 0
            p = self.peers if self.peers is not None else 0
            lines.append(f"🌱 Seeders: {s} | 👥 Peers: {p}")

        return "\n".join(lines)


class ActiveJob:
    """Represents a queued or in-flight transfer with cancellation support."""

    def __init__(
        self,
        job_id: str,
        job_type: str,
        chat_id: int,
        source: Any,
        reply_to: int,
        sender_id: int,
        is_file: bool = False,
        status_msg: Any = None,
        title: str = "Transfer",
    ):
        self.job_id = job_id
        self.job_type = job_type  # "tg_file" or "torrent"
        self.chat_id = chat_id
        self.source = source
        self.reply_to = reply_to
        self.sender_id = sender_id
        self.is_file = is_file
        self.status_msg = status_msg
        self.title = title
        self.cancel_event = asyncio.Event()
        self.thread_cancel_event = threading.Event()
        self.cancelled_by: Optional[int] = None
        self.task: Optional[asyncio.Task] = None
        self.clean_paths: list[str] = []

        # Torrent multi-file selection state
        self.torrent_files: list[dict] = []
        self.selected_indices: set[int] = set()
        self.page: int = 0
        self.selection_event: asyncio.Event = asyncio.Event()

    def cancel(self, cancelled_by: Optional[int] = None):
        self.cancelled_by = cancelled_by
        self.cancel_event.set()
        self.thread_cancel_event.set()
        if self.task and not self.task.done():
            self.task.cancel()


active_jobs: Dict[str, ActiveJob] = {}


def render_file_selector(job: ActiveJob, page_size: int = 6) -> tuple[str, list[list[Button]]]:
    """Render text and inline keyboard grid for interactive torrent file selection."""
    files = job.torrent_files
    total_files = len(files)
    total_bytes = sum(int(f.get("length", 0)) for f in files)
    selected_bytes = sum(
        int(f.get("length", 0))
        for f in files
        if int(f.get("index", 1)) in job.selected_indices
    )
    total_pages = max(1, (total_files + page_size - 1) // page_size)
    job.page = max(0, min(job.page, total_pages - 1))

    start_idx = job.page * page_size
    end_idx = min(start_idx + page_size, total_files)
    page_files = files[start_idx:end_idx]

    lines = [
        "📂 **Select Files to Download**",
        f"`{job.title}`",
        f"**Total**: {total_files} files ({human(total_bytes)}) | **Selected**: {len(job.selected_indices)} ({human(selected_bytes)})\n",
    ]

    for f in page_files:
        idx = int(f.get("index", 1))
        icon = "✅" if idx in job.selected_indices else "⬜"
        name = os.path.basename(f.get("path", f"file_{idx}"))
        fsize = human(int(f.get("length", 0)))
        lines.append(f"{idx}. {icon} `{safe_name(name, 'file')}` ({fsize})")

    lines.append(f"\n📄 **Page {job.page + 1}/{total_pages}** (Files {start_idx + 1} - {end_idx} of {total_files})")

    buttons = []

    # 1. Numbered Toggle Buttons (3 per row)
    toggle_row = []
    for f in page_files:
        idx = int(f.get("index", 1))
        check = "✅" if idx in job.selected_indices else "⬜"
        toggle_row.append(Button.inline(f"{idx} {check}", data=f"tsel:{job.job_id}:tog:{idx}"))
        if len(toggle_row) == 3:
            buttons.append(toggle_row)
            toggle_row = []
    if toggle_row:
        buttons.append(toggle_row)

    # 2. Pagination Buttons (if > 1 page)
    if total_pages > 1:
        prev_page = job.page - 1
        next_page = job.page + 1
        page_buttons = []
        if job.page > 0:
            page_buttons.append(Button.inline("◀ Prev", data=f"tsel:{job.job_id}:page:{prev_page}"))
        else:
            page_buttons.append(Button.inline("◀", data=f"tsel:{job.job_id}:noop"))

        page_buttons.append(Button.inline(f"{job.page + 1}/{total_pages}", data=f"tsel:{job.job_id}:noop"))

        if job.page < total_pages - 1:
            page_buttons.append(Button.inline("Next ▶", data=f"tsel:{job.job_id}:page:{next_page}"))
        else:
            page_buttons.append(Button.inline("▶", data=f"tsel:{job.job_id}:noop"))
        buttons.append(page_buttons)

    # 3. Bulk Action Buttons
    buttons.append([
        Button.inline("Select All ✅", data=f"tsel:{job.job_id}:all"),
        Button.inline("Deselect All ⬜", data=f"tsel:{job.job_id}:none"),
    ])

    # 4. Start & Cancel Buttons
    buttons.append([
        Button.inline(f"🚀 Start Download ({len(job.selected_indices)})", data=f"tsel:{job.job_id}:start"),
        Button.inline("Cancel ❌", data=f"cancel:{job.job_id}"),
    ])

    return "\n".join(lines), buttons


async def ticker(status_msg, prog, title, stop, job_id=None):
    """Edit the status message on an interval until `stop` is set."""
    last = ""
    buttons = [Button.inline("Cancel ❌", data=f"cancel:{job_id}")] if job_id else None
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
            await status_msg.edit(text, buttons=buttons)
            last = text
        except (MessageNotModifiedError, FloodWaitError):
            pass
        except Exception as exc:
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


def clean_job_paths(paths: list[str]):
    """Purge all files, directories, and partial temp files associated with a job."""
    for p in paths:
        if not p:
            continue
        try:
            if os.path.exists(p):
                if os.path.isdir(p):
                    shutil.rmtree(p, ignore_errors=True)
                else:
                    os.remove(p)
            # Check for partial/temp extensions
            for ext in (".temp", ".part", ".aria2", ".crdownload"):
                partial = p + ext
                if os.path.exists(partial):
                    if os.path.isdir(partial):
                        shutil.rmtree(partial, ignore_errors=True)
                    else:
                        os.remove(partial)
        except OSError as exc:
            log.warning("could not remove %s: %s", p, exc)


async def handle_tg_file_job(job: ActiveJob):
    """Process a Telegram file upload job by editing its existing status message."""
    buttons = [Button.inline("Cancel ❌", data=f"cancel:{job.job_id}")]
    status = job.status_msg
    if not status:
        status = await bot.send_message(job.chat_id, "Starting download...", reply_to=job.reply_to, buttons=buttons)
        job.status_msg = status

    path = None
    msg_id = job.source
    try:
        probe = await bot.get_messages(job.chat_id, ids=msg_id)
        if probe is None or probe.file is None:
            await status.edit("That message no longer has a file attached.", buttons=None)
            return

        size = probe.file.size or 0
        name = safe_name(probe.file.name, f"telegram_{msg_id}{probe.file.ext or ''}")
        job.title = name
        mime = probe.file.mime_type or mimetypes.guess_type(name)[0]

        free = shutil.disk_usage(config.DOWNLOAD_DIR).free
        if free < size * 1.05:
            await status.edit(
                f"Not enough disk space: need ~{human(size * 1.05)}, "
                f"{human(free)} free in {config.DOWNLOAD_DIR}.",
                buttons=None,
            )
            return

        message, via = await resolve_source(job.chat_id, msg_id, size)
        client = bot if via == "bot" else user
        path = os.path.join(config.DOWNLOAD_DIR, f"{msg_id}_{name}")
        job.clean_paths.append(path)

        prog = Progress()
        stop = asyncio.Event()
        prog.begin(f"downloading (via {via})", size)
        await status.edit(prog.render(name), buttons=buttons)
        tick = asyncio.create_task(ticker(status, prog, name, stop, job.job_id))
        try:
            def on_dl_progress(done, total):
                if job.cancel_event.is_set():
                    raise TransferCancelled("Download cancelled by user.")
                prog.update(done, total)

            # Download from Telegram
            dl_task = asyncio.create_task(client.download_media(message, file=path, progress_callback=on_dl_progress))
            await dl_task

            if job.cancel_event.is_set():
                raise TransferCancelled("Download cancelled by user.")

            prog.begin("uploading to Drive", os.path.getsize(path))
            loop = asyncio.get_running_loop()

            def on_chunk(done, total):
                loop.call_soon_threadsafe(prog.update, done, total)

            meta = await asyncio.to_thread(
                drive.upload,
                path,
                name,
                mime,
                None,
                on_chunk,
                job.thread_cancel_event,
            )
        finally:
            stop.set()
            await tick

        link = meta.get("webViewLink") or f"https://drive.google.com/file/d/{meta['id']}/view"
        await status.edit(
            f"**Uploaded**\n`{name}`\n{human(size)}\n\n{link}",
            buttons=None,
            link_preview=False,
        )
        log.info("uploaded %s (%s) as %s", name, human(size), meta["id"])

    except (TransferCancelled, asyncio.CancelledError):
        log.info("Job %s was cancelled", job.job_id)
        try:
            await status.edit(f"❌ **Transfer Cancelled**\n`{job.title}`", buttons=None)
        except Exception:
            pass
    except Exception as exc:
        log.exception("job failed")
        try:
            await status.edit(f"**Failed**\n`{type(exc).__name__}`\n{exc}", buttons=None)
        except Exception:
            pass
    finally:
        clean_job_paths(job.clean_paths)


async def handle_torrent_job(job: ActiveJob):
    """Process a BitTorrent / Magnet upload job by editing its existing status message."""
    buttons = [Button.inline("Cancel ❌", data=f"cancel:{job.job_id}")]
    status = job.status_msg
    if not status:
        status = await bot.send_message(job.chat_id, "Starting torrent download...", reply_to=job.reply_to, buttons=buttons)
        job.status_msg = status

    source = job.source
    is_torrent_file = job.is_file
    prog = Progress()
    stop = asyncio.Event()
    tick = None
    target_path = None
    torrent_title = "Resolving magnet metadata..." if not is_torrent_file else os.path.basename(source)
    job.title = torrent_title

    if is_torrent_file:
        job.clean_paths.append(source)

    try:
        prog.begin("connecting to swarm", 0)
        # Edit the queued message to show connecting/downloading status
        await status.edit("Connecting to swarm / Resolving metadata...", buttons=buttons)
        tick = asyncio.create_task(ticker(status, prog, torrent_title, stop, job.job_id))

        loop = asyncio.get_running_loop()

        def on_torrent_progress(stats: dict):
            if "name" in stats and stats["name"]:
                job.title = stats["name"]
            loop.call_soon_threadsafe(prog.update_torrent, stats)

        # 1. Download via aria2c
        async def on_file_selection(files: list[dict], t_name: str) -> list[int]:
            nonlocal tick
            # Stop the progress ticker while user is interacting with the selector
            stop.set()
            if tick:
                await tick
                tick = None

            job.torrent_files = files
            job.title = t_name
            # Default ALL files selected
            job.selected_indices = {int(f.get("index", i + 1)) for i, f in enumerate(files)}
            job.page = 0
            job.selection_event.clear()

            text, selector_buttons = render_file_selector(job)
            await status.edit(text, buttons=selector_buttons)

            # Wait until user confirms selection or cancels
            sel_task = asyncio.create_task(job.selection_event.wait())
            cancel_task = asyncio.create_task(job.cancel_event.wait())
            try:
                await asyncio.wait(
                    [sel_task, cancel_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                if not sel_task.done():
                    sel_task.cancel()
                if not cancel_task.done():
                    cancel_task.cancel()

            if job.cancel_event.is_set():
                raise TransferCancelled("Torrent download cancelled by user during file selection.")

            # Selection confirmed - restart ticker
            stop.clear()
            prog.begin("downloading (torrent)", 0)
            tick = asyncio.create_task(ticker(status, prog, job.title, stop, job.job_id))
            return list(job.selected_indices)

        result = await torrent_manager.download(
            source,
            config.DOWNLOAD_DIR,
            progress_cb=on_torrent_progress,
            cancel_event=job.cancel_event,
            file_selection_cb=on_file_selection,
        )

        target_path = result["path"]
        torrent_title = result["name"]
        job.title = torrent_title
        is_dir = result["is_dir"]
        total_size = result["total_size"]
        job.clean_paths.append(target_path)

        # 2. Upload to Google Drive
        prog.begin("uploading to Drive", total_size)

        def on_drive_chunk(done, total):
            loop.call_soon_threadsafe(prog.update, done, total)

        if is_dir:
            meta = await asyncio.to_thread(
                drive.upload_folder,
                target_path,
                torrent_title,
                None,
                on_drive_chunk,
                job.thread_cancel_event,
            )
            link = meta.get("webViewLink") or f"https://drive.google.com/drive/folders/{meta['id']}"
            stop.set()
            if tick:
                await tick
            await status.edit(
                f"📁 **Uploaded Folder**\n`{torrent_title}`\n{human(total_size)}\n\n{link}",
                buttons=None,
                link_preview=False,
            )
        else:
            mime = mimetypes.guess_type(target_path)[0]
            meta = await asyncio.to_thread(
                drive.upload,
                target_path,
                torrent_title,
                mime,
                None,
                on_drive_chunk,
                job.thread_cancel_event,
            )
            link = meta.get("webViewLink") or f"https://drive.google.com/file/d/{meta['id']}/view"
            stop.set()
            if tick:
                await tick
            await status.edit(
                f"**Uploaded**\n`{torrent_title}`\n{human(total_size)}\n\n{link}",
                buttons=None,
                link_preview=False,
            )

        log.info("uploaded torrent %s (%s) as %s", torrent_title, human(total_size), meta["id"])

    except (TransferCancelled, asyncio.CancelledError):
        log.info("Torrent job %s was cancelled", job.job_id)
        stop.set()
        if tick:
            await tick
        try:
            await status.edit(f"❌ **Torrent Cancelled**\n`{job.title}`", buttons=None)
        except Exception:
            pass
    except Aria2NotInstalledError as exc:
        log.error("aria2 not installed: %s", exc)
        stop.set()
        if tick:
            await tick
        await status.edit(f"⚠️ **Torrent Engine Missing**\n\n{exc}", buttons=None)
    except Exception as exc:
        log.exception("torrent job failed")
        stop.set()
        if tick:
            await tick
        try:
            await status.edit(f"**Torrent Failed**\n`{type(exc).__name__}`\n{exc}", buttons=None)
        except Exception:
            pass
    finally:
        clean_job_paths(job.clean_paths)


async def worker():
    """One transfer at a time - bounds disk, RAM and bandwidth."""
    while True:
        job: ActiveJob = await queue.get()
        try:
            if job.cancel_event.is_set():
                if job.status_msg:
                    try:
                        await job.status_msg.edit(f"❌ **Cancelled (from queue)**\n`{job.title}`", buttons=None)
                    except Exception:
                        pass
                clean_job_paths(job.clean_paths)
                continue

            job.task = asyncio.current_task()
            if job.job_type == "tg_file":
                await handle_tg_file_job(job)
            elif job.job_type == "torrent":
                await handle_torrent_job(job)
        finally:
            active_jobs.pop(job.job_id, None)
            queue.task_done()


@bot.on(events.CallbackQuery(pattern=rb"^tsel:([^:]+):(.+)"))
async def on_torrent_select_callback(event):
    job_id = event.pattern_match.group(1).decode("utf-8")
    action = event.pattern_match.group(2).decode("utf-8")

    if not is_trusted(event):
        await event.answer("Not authorised to modify this transfer.", alert=True)
        return

    job = active_jobs.get(job_id)
    if not job or not job.torrent_files:
        await event.answer("File selection is no longer active.", alert=True)
        return

    if action.startswith("tog:"):
        idx = int(action.split(":")[1])
        if idx in job.selected_indices:
            job.selected_indices.remove(idx)
        else:
            job.selected_indices.add(idx)
        await event.answer()
    elif action.startswith("page:"):
        target_page = int(action.split(":")[1])
        job.page = target_page
        await event.answer()
    elif action == "all":
        job.selected_indices = {int(f.get("index", i + 1)) for i, f in enumerate(job.torrent_files)}
        await event.answer("All files selected.")
    elif action == "none":
        job.selected_indices.clear()
        await event.answer("All files deselected.")
    elif action == "start":
        if not job.selected_indices:
            await event.answer("⚠️ Please select at least 1 file to download!", alert=True)
            return
        job.selection_event.set()
        await event.answer("🚀 Starting download...")
        return
    elif action == "noop":
        await event.answer()
        return

    # Re-render selector and edit status message
    text, buttons = render_file_selector(job)
    try:
        await job.status_msg.edit(text, buttons=buttons)
    except (MessageNotModifiedError, FloodWaitError):
        pass
    except Exception as exc:
        log.debug("Selector edit failed: %s", exc)


@bot.on(events.CallbackQuery(pattern=rb"^cancel:(.+)"))
async def on_cancel_callback(event):
    job_id = event.data_match.group(1).decode("utf-8")
    if not is_trusted(event):
        await event.answer("Not authorised to cancel this transfer.", alert=True)
        return

    job = active_jobs.get(job_id)
    if job:
        job.cancel(cancelled_by=event.sender_id)
        await event.answer("Cancelling transfer...")
        # If job is still in queue, immediately edit its message
        if job.status_msg and job.task is None:
            try:
                await job.status_msg.edit(f"❌ **Cancelled (from queue)**\n`{job.title}`", buttons=None)
            except Exception:
                pass
    else:
        await event.answer("Job is no longer active or already finished.", alert=True)


@bot.on(events.NewMessage(pattern=r"^/cancel$"))
async def on_cancel_command(event):
    if not is_trusted(event):
        await reply_not_authorised(event)
        return

    matching = [j for j in active_jobs.values() if j.chat_id == event.chat_id]
    if not matching:
        matching = list(active_jobs.values())

    if not matching:
        await event.reply("No active transfers running.")
        return

    for j in matching:
        j.cancel(cancelled_by=event.sender_id)
        if j.status_msg and j.task is None:
            try:
                await j.status_msg.edit(f"❌ **Cancelled (from queue)**\n`{j.title}`", buttons=None)
            except Exception:
                pass

    await event.reply(f"Cancelled {len(matching)} transfer(s).")


def get_dir_size(path: str) -> int:
    """Calculate total size of directory in bytes."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if not os.path.islink(fp) and os.path.exists(fp):
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
    return total


def list_download_items(path: str) -> list[dict]:
    """List all top-level files and folders inside download directory."""
    if not os.path.exists(path):
        return []
    items = []
    try:
        entries = list(os.scandir(path))
    except Exception:
        return []

    for entry in sorted(entries, key=lambda e: e.stat().st_mtime if os.path.exists(e.path) else 0, reverse=True):
        if entry.name.startswith("."):
            continue
        try:
            if entry.is_dir(follow_symlinks=False):
                size = get_dir_size(entry.path)
                items.append({"name": entry.name, "is_dir": True, "size": size})
            elif entry.is_file(follow_symlinks=False):
                size = entry.stat().st_size
                items.append({"name": entry.name, "is_dir": False, "size": size})
        except OSError:
            pass
    return items


def purge_download_dir(path: str) -> tuple[int, int]:
    """Delete all items inside downloads directory and return (count, bytes_freed)."""
    if not os.path.exists(path):
        return 0, 0
    deleted_count = 0
    bytes_freed = 0
    try:
        entries = list(os.scandir(path))
    except Exception:
        return 0, 0

    for entry in entries:
        try:
            if entry.is_dir(follow_symlinks=False):
                size = get_dir_size(entry.path)
                shutil.rmtree(entry.path, ignore_errors=True)
                deleted_count += 1
                bytes_freed += size
            else:
                size = entry.stat().st_size
                os.remove(entry.path)
                deleted_count += 1
                bytes_freed += size
        except OSError as exc:
            log.warning("could not delete %s: %s", entry.path, exc)
    return deleted_count, bytes_freed


@bot.on(events.NewMessage(pattern=r"^/(?:files|storage|driveinfo|disk|ls)$"))
async def on_files_command(event):
    if not is_trusted(event):
        await reply_not_authorised(event)
        return

    status_msg = await event.reply("Fetching storage info...")

    # 1. Fetch Google Drive Quota
    drive_info = None
    try:
        drive_info = await asyncio.to_thread(drive.get_storage_quota)
    except Exception as exc:
        log.warning("Could not fetch Google Drive quota: %s", exc)

    # 2. Fetch VPS Disk Usage
    try:
        usage = shutil.disk_usage(config.DOWNLOAD_DIR)
        total_disk = usage.total
        free_disk = usage.free
        used_disk = usage.used
        free_pct = (free_disk / total_disk * 100) if total_disk else 0
        used_pct = (used_disk / total_disk * 100) if total_disk else 0
    except Exception as exc:
        await status_msg.edit(f"Could not read disk usage: {exc}")
        return

    # 3. List Local Download Items
    items = list_download_items(config.DOWNLOAD_DIR)
    dir_size = sum(item["size"] for item in items)

    vps_bar_filled = min(max(int(used_pct // 5), 0), 20)
    vps_bar = f"`[{'#' * vps_bar_filled}{'.' * (20 - vps_bar_filled)}]` {used_pct:.1f}% used"

    lines = ["💾 **Storage Overview**\n"]

    # Render Google Drive Section
    if drive_info:
        email_tag = f" ({drive_info['user_email']})" if drive_info.get("user_email") else ""
        lines.append(f"🌐 **Google Drive Storage**{email_tag}:")
        if drive_info["limit"] > 0:
            d_pct = drive_info["usage_pct"]
            d_bar_filled = min(max(int(d_pct // 5), 0), 20)
            d_bar = f"`[{'#' * d_bar_filled}{'.' * (20 - d_bar_filled)}]` {d_pct:.1f}% used"
            d_free_pct = (drive_info["free"] / drive_info["limit"] * 100) if drive_info["limit"] else 0
            lines.append(d_bar)
            lines.append(f"• **Free Space**: `{human(drive_info['free'])}` ({d_free_pct:.1f}% free)")
            lines.append(f"• **Used Space**: `{human(drive_info['usage'])}` (Drive: `{human(drive_info['usage_in_drive'])}`, Trash: `{human(drive_info['usage_in_trash'])}`)")
            lines.append(f"• **Total Quota**: `{human(drive_info['limit'])}`")
        else:
            lines.append(f"• **Used Space**: `{human(drive_info['usage'])}` (Drive: `{human(drive_info['usage_in_drive'])}`, Trash: `{human(drive_info['usage_in_trash'])}`)")
            lines.append("• **Total Quota**: Unlimited")
        lines.append("")

    # Render VPS Local Disk Section
    lines.append("🖥️ **VPS Local Disk**:")
    lines.append(vps_bar)
    lines.append(f"• **Free Space**: `{human(free_disk)}` ({free_pct:.1f}% free)")
    lines.append(f"• **Used Space**: `{human(used_disk)}`")
    lines.append(f"• **Total Disk**: `{human(total_disk)}`")
    lines.append(f"• **Downloads Folder**: `{human(dir_size)}` ({len(items)} item(s) in `{config.DOWNLOAD_DIR}`)")
    lines.append("")

    # Render Downloaded Files List
    if not items:
        lines.append("📂 **Files in Downloads**: _Folder is currently empty._")
    else:
        lines.append(f"📂 **Files in Downloads** ({len(items)}):")
        for i, item in enumerate(items[:20], 1):
            icon = "📁" if item["is_dir"] else "📄"
            name = item["name"]
            if len(name) > 38:
                name = name[:35] + "..."
            lines.append(f"{i}. {icon} `{name}` ({human(item['size'])})")
        if len(items) > 20:
            lines.append(f"_...and {len(items) - 20} more items_")

    await status_msg.edit("\n".join(lines))


@bot.on(events.NewMessage(pattern=r"^/(start|help)"))
async def on_start(event):
    await event.reply(
        "👋 **Telegram & Torrent to Google Drive Bot**\n\n"
        "**Send me:**\n"
        f"• **File**: Directly up to {human(config.BOT_DOWNLOAD_LIMIT)} (or larger in shared groups)\n"
        "• **Magnet link**: Send `magnet:?xt=urn:...` or `/magnet <link>`\n"
        "• **.torrent file**: Upload any `.torrent` file\n\n"
        "**Commands:**\n"
        "/files - show Google Drive & VPS storage + downloaded files\n"
        "/cancel - cancel in-progress transfer\n"
        "/id - show your Telegram user id & chat id\n"
        "/wipe - permanently delete files in Drive folder + clean downloads"
    )


@bot.on(events.NewMessage(pattern=r"^/id"))
async def on_id(event):
    await event.reply(f"Your user id: `{event.sender_id}`\nChat id: `{event.chat_id}`")


def is_trusted(event):
    return event.sender_id in config.ALLOWED_USERS or event.chat_id in config.ALLOWED_CHATS


async def reply_not_authorised(event):
    log.warning(
        "rejected: sender_id=%r chat_id=%r; ALLOWED_USERS=%r ALLOWED_CHATS=%r",
        event.sender_id, event.chat_id, config.ALLOWED_USERS, config.ALLOWED_CHATS,
    )
    await event.reply(
        f"Not authorised.\nYour id: `{event.sender_id}`\nThis chat's id: `{event.chat_id}`\n"
        "Add one of those to ALLOWED_USERS or ALLOWED_CHATS and restart the bot."
    )


@bot.on(events.NewMessage(pattern=r"^/wipe(?:\s+(\S+))?$"))
async def on_wipe(event):
    if not is_trusted(event):
        await reply_not_authorised(event)
        return
    if not config.DRIVE_FOLDER_ID:
        await event.reply(
            "DRIVE_FOLDER_ID is not set in .env - refusing to wipe My Drive root."
        )
        return

    confirmed = (event.pattern_match.group(1) or "").lower() == "confirm"
    if not confirmed:
        status = await event.reply("Checking Drive & local downloads folder...")
        try:
            count = await asyncio.to_thread(drive.count_wipe_targets, config.DRIVE_FOLDER_ID)
        except Exception as exc:
            log.exception("wipe count failed")
            await status.edit(f"Could not check the Drive folder: {exc}")
            return

        local_items = list_download_items(config.DOWNLOAD_DIR)
        local_size = sum(x["size"] for x in local_items)

        if count == 0 and len(local_items) == 0:
            await status.edit("Both Google Drive folder and local downloads are already empty. Nothing to do.")
            return

        await status.edit(
            f"⚠️ **Wipe Confirmation**\n\n"
            f"This will **permanently** delete:\n"
            f"• **Google Drive**: `{count}` file(s) in Drive folder (including its trash)\n"
            f"• **Local Downloads**: `{len(local_items)}` item(s) (`{human(local_size)}`) in `{config.DOWNLOAD_DIR}`\n\n"
            "This cannot be undone.\n\nSend `/wipe confirm` to proceed."
        )
        return

    status = await event.reply("Wiping Google Drive folder and purging local downloads...")
    prog = Progress()
    stop = asyncio.Event()
    tick = asyncio.create_task(ticker(status, prog, "Wiping Drive folder", stop))
    try:
        prog.begin("deleting", 0)
        loop = asyncio.get_running_loop()

        def on_progress(done, total):
            loop.call_soon_threadsafe(prog.update, done, total)

        result = await asyncio.to_thread(drive.wipe_folder, config.DRIVE_FOLDER_ID, on_progress)
    except Exception as exc:
        log.exception("wipe failed")
        stop.set()
        await tick
        await status.edit(f"**Wipe failed**\n`{type(exc).__name__}`\n{exc}")
        return
    else:
        stop.set()
        await tick

    # Also purge local downloads directory
    local_deleted, local_freed = purge_download_dir(config.DOWNLOAD_DIR)

    lines = [
        "🧹 **Wipe Complete**",
        f"• **Google Drive**: Deleted {result['deleted']}/{result['total']} file(s).",
        f"• **Local VPS**: Purged {local_deleted} download item(s) ({human(local_freed)} freed).",
    ]
    if result["errors"]:
        log.warning("wipe errors: %s", result["errors"])
        lines.append(f"\n⚠️ {len(result['errors'])} Drive deletion error(s):")
        lines.extend(f"- {e}" for e in result["errors"][:5])
        if len(result["errors"]) > 5:
            lines.append(f"...and {len(result['errors']) - 5} more (see logs).")
    await status.edit("\n".join(lines))


@bot.on(events.NewMessage(func=lambda e: e.file is not None))
async def on_file(event):
    if not is_trusted(event):
        await reply_not_authorised(event)
        return

    job_id = uuid.uuid4().hex[:8]
    buttons = [Button.inline("Cancel ❌", data=f"cancel:{job_id}")]
    file_name = event.file.name or ""
    q_len = queue.qsize()

    # Check if this is a .torrent file
    if file_name.lower().endswith(".torrent"):
        torrent_tmp = os.path.join(config.DOWNLOAD_DIR, f"{event.id}_{safe_name(file_name, 'file.torrent')}")
        await bot.download_media(event.message, file=torrent_tmp)
        msg_text = f"Queued torrent file ({q_len} ahead in queue)..." if q_len > 0 else "Queued torrent file..."
        status_msg = await event.reply(msg_text, buttons=buttons)

        job = ActiveJob(
            job_id=job_id,
            job_type="torrent",
            chat_id=event.chat_id,
            source=torrent_tmp,
            reply_to=event.id,
            sender_id=event.sender_id,
            is_file=True,
            status_msg=status_msg,
            title=file_name,
        )
        active_jobs[job_id] = job
        await queue.put(job)
        return

    # Regular Telegram file
    msg_text = f"Queued ({q_len} ahead in queue)..." if q_len > 0 else "Queued..."
    status_msg = await event.reply(msg_text, buttons=buttons)

    job = ActiveJob(
        job_id=job_id,
        job_type="tg_file",
        chat_id=event.chat_id,
        source=event.id,
        reply_to=event.id,
        sender_id=event.sender_id,
        is_file=False,
        status_msg=status_msg,
        title=file_name or f"telegram_{event.id}",
    )
    active_jobs[job_id] = job
    await queue.put(job)


@bot.on(events.NewMessage(pattern=MAGNET_REGEX))
async def on_magnet(event):
    if not is_trusted(event):
        await reply_not_authorised(event)
        return

    magnet_uri = event.pattern_match.group(1).strip()
    job_id = uuid.uuid4().hex[:8]
    buttons = [Button.inline("Cancel ❌", data=f"cancel:{job_id}")]
    q_len = queue.qsize()

    msg_text = f"Queued torrent magnet ({q_len} ahead in queue)..." if q_len > 0 else "Queued torrent magnet..."
    status_msg = await event.reply(msg_text, buttons=buttons)

    job = ActiveJob(
        job_id=job_id,
        job_type="torrent",
        chat_id=event.chat_id,
        source=magnet_uri,
        reply_to=event.id,
        sender_id=event.sender_id,
        is_file=False,
        status_msg=status_msg,
        title="Magnet Torrent",
    )
    active_jobs[job_id] = job
    await queue.put(job)


async def main():
    log.info(
        "ALLOWED_USERS=%r ALLOWED_CHATS=%r", config.ALLOWED_USERS, config.ALLOWED_CHATS
    )
    if not config.ALLOWED_USERS and not config.ALLOWED_CHATS:
        log.warning("ALLOWED_USERS and ALLOWED_CHATS are both empty - everything will be rejected.")

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

    # Pre-check aria2c availability
    if shutil.which("aria2c"):
        log.info("aria2c found in PATH - torrent engine ready")
    else:
        log.warning("aria2c NOT found in PATH. Install via `sudo apt install aria2` for torrent support.")

    worker_task = asyncio.create_task(worker())

    try:
        await bot.run_until_disconnected()
    finally:
        worker_task.cancel()
        await torrent_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
