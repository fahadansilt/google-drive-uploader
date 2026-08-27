"""Configuration loaded from the environment (.env)."""
import os
from dotenv import load_dotenv

load_dotenv()


def _int(name, default):
    raw = os.getenv(name, "").strip()
    return int(raw) if raw else default


def _req(name):
    val = os.getenv(name, "").strip()
    if not val:
        raise SystemExit(f"Missing required setting {name} (see .env.example)")
    return val


TG_API_ID = int(_req("TG_API_ID"))
TG_API_HASH = _req("TG_API_HASH")
BOT_TOKEN = _req("BOT_TOKEN")
USER_SESSION = os.getenv("USER_SESSION", "").strip()

def _id_set(name):
    raw = os.getenv(name, "")
    try:
        return {int(v) for v in raw.replace(" ", "").split(",") if v}
    except ValueError as exc:
        raise SystemExit(f"{name}={raw!r} contains a non-numeric value: {exc}") from exc


ALLOWED_USERS = _id_set("ALLOWED_USERS")
# Channels/groups trusted wholesale - needed because posts made as the channel
# itself (or by an anonymous group admin) carry sender_id=None, so there is no
# per-user id to check. Only add chats only you/trusted admins can post in.
ALLOWED_CHATS = _id_set("ALLOWED_CHATS")

GOOGLE_CLIENT_ID = _req("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = _req("GOOGLE_CLIENT_SECRET")
GOOGLE_TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json").strip()
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip() or None

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads").strip()
UPLOAD_CHUNK = _int("UPLOAD_CHUNK_MB", 16) * 1024 * 1024
BOT_DOWNLOAD_LIMIT = _int("BOT_DOWNLOAD_LIMIT_MB", 2000) * 1024 * 1024

# Per-file access to what this app creates. If uploads into DRIVE_FOLDER_ID are
# rejected, widen to "https://www.googleapis.com/auth/drive".
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
