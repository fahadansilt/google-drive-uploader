"""Google Drive resumable uploads using an OAuth desktop client."""
import os
import time
import json
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

import config

log = logging.getLogger("drive")

RETRYABLE = {500, 502, 503, 504, 429}


def load_credentials():
    """Load token.json, refreshing it if the access token has expired."""
    if not os.path.exists(config.GOOGLE_TOKEN_FILE):
        raise SystemExit(
            f"{config.GOOGLE_TOKEN_FILE} not found. Run:  python auth_drive.py"
        )

    with open(config.GOOGLE_TOKEN_FILE, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # Client id/secret always come from the environment so the token file stays
    # portable and the secret lives in exactly one place.
    data["client_id"] = config.GOOGLE_CLIENT_ID
    data["client_secret"] = config.GOOGLE_CLIENT_SECRET

    creds = Credentials.from_authorized_user_info(data, config.SCOPES)
    if not creds.valid:
        if not (creds.expired and creds.refresh_token):
            raise SystemExit("Drive token is invalid. Re-run:  python auth_drive.py")
        creds.refresh(Request())
        save_credentials(creds)
    return creds


def save_credentials(creds):
    with open(config.GOOGLE_TOKEN_FILE, "w", encoding="utf-8") as fh:
        fh.write(creds.to_json())
    os.chmod(config.GOOGLE_TOKEN_FILE, 0o600)


def build_service():
    return build("drive", "v3", credentials=load_credentials(), cache_discovery=False)


def upload(path, name, mime_type=None, progress_cb=None):
    """Upload `path` to Drive as `name`. Returns the created file's metadata.

    Runs blocking, so callers on the event loop should hand it to a thread.
    """
    service = build_service()
    body = {"name": name}
    if config.DRIVE_FOLDER_ID:
        body["parents"] = [config.DRIVE_FOLDER_ID]

    media = MediaFileUpload(
        path,
        mimetype=mime_type or "application/octet-stream",
        chunksize=config.UPLOAD_CHUNK,
        resumable=True,
    )
    request = service.files().create(
        body=body,
        media_body=media,
        fields="id,name,size,webViewLink",
        supportsAllDrives=True,
    )

    total = os.path.getsize(path)
    response = None
    attempt = 0

    while response is None:
        try:
            status, response = request.next_chunk(num_retries=3)
            attempt = 0
            if status and progress_cb:
                progress_cb(status.resumable_progress, total)
        except HttpError as exc:
            if exc.resp.status not in RETRYABLE or attempt >= 5:
                raise
            attempt += 1
            delay = 2 ** attempt
            log.warning("Drive %s, retrying in %ss", exc.resp.status, delay)
            time.sleep(delay)
        except (TimeoutError, ConnectionError, OSError) as exc:
            if attempt >= 5:
                raise
            attempt += 1
            delay = 2 ** attempt
            log.warning("Upload interrupted (%s), resuming in %ss", exc, delay)
            time.sleep(delay)

    if progress_cb:
        progress_cb(total, total)
    return response
