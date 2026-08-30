# drive-bot

Telegram → Google Drive. Receives a file, streams it to disk, pushes it to Drive
with a resumable upload, replies with the link, deletes the temp file.

Built on **MTProto (Telethon)**, not the HTTP Bot API — that is what makes files
above 20 MB possible at all.

## Why it looks like this

| Path | Ceiling | Why |
|---|---|---|
| HTTP Bot API `getFile` | **20 MB** | Hard Telegram limit. No workaround in code. |
| MTProto with a **bot token** | **~2 GB** | What this bot does by default. |
| MTProto with a **user session** | **4 GB** | Needed for Premium-sized files. |

The catch on the 4 GB path: a user account **cannot read your bot's private
DMs**. So for files over 2 GB the file must be posted in a **group or channel
that both the bot and the user account belong to**. The bot sees the update,
then hands the actual download to the user client. `resolve_source()` in
`bot.py` picks the tier automatically and tells you plainly when it can't.

Also note: only a **Telegram Premium** account can *send* a file above 2 GB in
the first place. That is the sender's account, not yours.

## Flow

```
Telegram ──update──▶ bot client (Telethon)
                         │
                    size ≤ 2 GB ? bot client : user client
                         │
                    download to DOWNLOAD_DIR  (progress → edited message)
                         │
                    Drive v3 resumable upload, 16 MiB chunks
                         │
                    reply with webViewLink, unlink temp file
```

One transfer at a time (single `worker()` reading an `asyncio.Queue`), which
bounds disk, RAM and bandwidth. Everything else queues behind it.

## Setup

### 1. Credentials

**Telegram MTProto** — <https://my.telegram.org> → API development tools →
copy `api_id` and `api_hash`.

**Bot token** — [@BotFather](https://t.me/BotFather) → `/newbot`. Then send it
`/setprivacy` → **Disable** if you want it to see files in groups.

**Google Drive** — Cloud Console → *APIs & Services*:
1. Enable the **Google Drive API**.
2. *OAuth consent screen* → External → add your own email as a **Test user**
   (a test-user token expires after 7 days; publish the app to stop that).
3. *Credentials* → Create credentials → **OAuth client ID** → **Desktop app**.
4. Copy the client ID and secret into `.env`.

### 2. Install

Needs **Python 3.10+** (`asyncio.to_thread`, loop-free `asyncio.Queue`).

```bash
sudo adduser --system --group --home /opt/drive-bot drivebot
sudo -u drivebot -H bash
cd /opt/drive-bot
git clone <your-repo> . || true          # or scp the files here
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env && nano .env
```

`cryptg` is in the requirements on purpose — it moves Telethon's AES into C and
is worth several multiples of throughput on large files.

### 3. Authorise Drive

`auth_drive.py` needs a browser once. On a headless VPS, tunnel the port:

```bash
ssh -L 8080:localhost:8080 you@vps      # from your laptop
# then, on the VPS:
venv/bin/python auth_drive.py           # open the printed URL locally
```

Alternatively run it on your laptop and `scp token.json` to the VPS. The token
is written `0600` and refreshes itself from then on.

### 4. Optional: the 4 GB path

```bash
venv/bin/python gen_session.py          # log in as your user account
# paste the printed value into USER_SESSION= in .env
```

Then make a private group, add the bot **and** be in it yourself, and post large
files there. `USER_SESSION` is a full account credential — it belongs in `.env`
at `0600`, never in git.

### 5. Run

```bash
venv/bin/python bot.py                  # foreground, to check it comes up
```

Send `/id` to the bot, put that number in `ALLOWED_USERS`, restart. Without it
every upload is rejected — that is deliberate, an open bot is an open bucket.

### 6. systemd

```bash
sudo cp drive-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now drive-bot
journalctl -u drive-bot -f
```

## Features & Usage

### 1. Telegram File Uploads
Send or forward any document / media to the bot:
- **Up to 2 GB**: Send directly in a private DM with the bot.
- **2 GB to 4 GB**: Post in a shared group/channel where both the bot and your configured user account sit.

### 2. BitTorrent & Magnet Links (`aria2c`)
- **Magnet link**: Send any magnet URI starting with `magnet:?xt=urn:...` or `/magnet <link>`.
- **.torrent file**: Send or upload any `.torrent` file directly.
- **Interactive Multi-File Selection**: For torrents with multiple files (e.g. TV seasons, album packs), the bot pauses after resolving metadata and presents an interactive Telegram menu with inline buttons:
  - **Default**: All files are selected (`✅`).
  - **Individual Toggles**: Select or deselect specific episodes/files with numbered buttons (`[ 1 ✅ ]`, `[ 2 ⬜ ]`).
  - **Bulk Actions**: `[ Select All ✅ ]` and `[ Deselect All ⬜ ]`.
  - **Pagination**: Navigate large torrents with `[ ◀ Prev ]` and `[ Next ▶ ]`.
  - **Execution**: Tap `[ 🚀 Start Download ]` to download only selected files, or `[ Cancel ❌ ]` to abort.
- **Single file torrent**: Uploaded directly to your Drive folder.
- **Multi-file torrent**: Uploaded into a dedicated subfolder in Google Drive matching the torrent title.

> **Requirement**: Make sure `aria2c` is installed on your VPS:
> ```bash
> sudo apt update && sudo apt install -y aria2
> ```

### 3. Storage & Downloaded Files (`/files`)
- **Google Drive Storage**: Displays total quota, used space (in Drive and Trash), free space, and usage percentage bar.
- **VPS Local Disk**: Displays available disk space, used space percentage bar, and downloads folder size.
- **Files on Disk**: Lists all active/stored files and folders in the downloads directory with their sizes.
- Aliases: `/files`, `/storage`, `/driveinfo`, `/disk`, `/ls`.

### 4. Interactive Transfer Cancellation
- Every active transfer message displays an inline **`[Cancel ❌]`** button.
- Clicking the cancel button or sending `/cancel` immediately halts in-progress Telegram downloads, torrent swarms, or Google Drive uploads and wipes partial data from the temporary disk.

## Wiping Drive folder & Local Downloads

`/wipe` permanently deletes every file in `DRIVE_FOLDER_ID` (including files in the trash that originated from that folder) **and** purges all files/folders inside the local VPS `DOWNLOAD_DIR`.

Scope for Drive is deliberately narrow: it only ever queries `'{DRIVE_FOLDER_ID}' in parents`, never a whole-account trash purge (`files.emptyTrash`), so files elsewhere in your Drive are never touched even in the trashed state — that also means it works within the existing `drive.file` OAuth scope, no re-consent needed.

Two-step by design:
```
/wipe            -> counts Drive and local targets, warns, does nothing yet
/wipe confirm    -> permanently deletes from Drive and purges local downloads
```
Refuses outright if `DRIVE_FOLDER_ID` is unset, rather than touching My Drive root. Gated by the same `ALLOWED_USERS` / `ALLOWED_CHATS` check as uploads.

## Disk

The whole file lands on disk before the upload starts, so you need
**headroom ≥ the largest file** you intend to move — 4 GB files mean at least
5 GB free in `DOWNLOAD_DIR`. `handle_tg_file_job()` and `torrent_manager` check
free space before downloading and refuse with a clear message rather than
filling the disk. Temp files and directories are safely cleaned up in all
scenarios (success, failure, or cancellation).

## Configuration

Everything lives in `.env`; see `.env.example` for the annotated list.

| Key | Notes |
|---|---|
| `ALLOWED_USERS` | Comma-separated user IDs. Empty = nobody. |
| `DRIVE_FOLDER_ID` | From the folder URL after `/folders/`. Empty = My Drive root. |
| `UPLOAD_CHUNK_MB` | Bigger is faster, and each chunk is held in RAM. Drop to 8 on a 512 MB VPS. |
| `BOT_DOWNLOAD_LIMIT_MB` | Tier switch point. 2000 unless Telegram changes. |
| `DOWNLOAD_DIR` | Put this on the roomiest volume. |

## Troubleshooting

**`403 insufficientFilePermissions` or the folder is rejected** — the app uses
the narrow `drive.file` scope. Widen `SCOPES` in `config.py` to
`https://www.googleapis.com/auth/drive`, delete `token.json`, re-run
`auth_drive.py`.

**Token dies every 7 days** — the OAuth consent screen is still in *Testing*.
Publish it.

**Bot ignores files in a group** — BotFather `/setprivacy` → Disable, then
remove and re-add the bot to the group.

**`Not authorised`** — your ID is missing from `ALLOWED_USERS`; `/id` prints it.

**Upload stalls near the end** — normal for large files; Drive finalises the
resumable session after the last chunk. The retry loop in `drive.py` handles
5xx/429 with exponential backoff and resumes from `resumable_progress` rather
than restarting the upload.
