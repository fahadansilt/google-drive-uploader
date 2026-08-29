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


class TransferCancelled(Exception):
    """Raised when a transfer is cancelled by user request."""


def upload(path, name, mime_type=None, parent_id=None, progress_cb=None, cancel_event=None):
    """Upload `path` to Drive as `name`. Returns the created file's metadata.

    Runs blocking, so callers on the event loop should hand it to a thread.
    """
    service = build_service()
    body = {"name": name}
    target_parent = parent_id or config.DRIVE_FOLDER_ID
    if target_parent:
        body["parents"] = [target_parent]

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
        if cancel_event and cancel_event.is_set():
            raise TransferCancelled("Drive upload cancelled by user.")

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


def create_folder(name, parent_id=None):
    """Create a folder in Google Drive and return its metadata."""
    service = build_service()
    body = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    target_parent = parent_id or config.DRIVE_FOLDER_ID
    if target_parent:
        body["parents"] = [target_parent]

    folder = service.files().create(
        body=body,
        fields="id,name,webViewLink",
        supportsAllDrives=True,
    ).execute()
    return folder


def upload_folder(local_dir, folder_name=None, parent_folder_id=None, progress_cb=None, cancel_event=None):
    """Recursively upload an entire directory to Google Drive.

    Returns the created top-level folder's metadata.
    """
    local_dir = os.path.abspath(local_dir)
    root_name = folder_name or os.path.basename(local_dir)

    # 1. Compute total size across all files
    all_files = []
    total_bytes = 0
    for root, _, filenames in os.walk(local_dir):
        for fname in filenames:
            fpath = os.path.join(root, fname)
            fsize = os.path.getsize(fpath)
            all_files.append((fpath, fsize))
            total_bytes += fsize

    # 2. Create root folder in Google Drive
    if cancel_event and cancel_event.is_set():
        raise TransferCancelled("Upload cancelled by user.")

    root_folder = create_folder(root_name, parent_id=parent_folder_id)
    root_folder_id = root_folder["id"]

    # Cache drive folder IDs by relative directory path
    folder_cache = {"": root_folder_id}

    uploaded_so_far = 0

    for fpath, fsize in all_files:
        if cancel_event and cancel_event.is_set():
            raise TransferCancelled("Upload cancelled by user.")

        rel_path = os.path.relpath(fpath, local_dir)
        rel_dir = os.path.dirname(rel_path)
        file_name = os.path.basename(fpath)

        # Ensure subfolder hierarchy exists in Drive
        if rel_dir not in folder_cache:
            current_parent = root_folder_id
            parts = rel_dir.replace("\\", "/").split("/")
            accumulated = ""
            for part in parts:
                accumulated = os.path.join(accumulated, part) if accumulated else part
                if accumulated not in folder_cache:
                    new_folder = create_folder(part, parent_id=current_parent)
                    folder_cache[accumulated] = new_folder["id"]
                current_parent = folder_cache[accumulated]

        target_parent = folder_cache[rel_dir]

        def file_progress(done_in_file, total_in_file):
            if progress_cb:
                progress_cb(uploaded_so_far + done_in_file, total_bytes)

        upload(
            fpath,
            file_name,
            parent_id=target_parent,
            progress_cb=file_progress,
            cancel_event=cancel_event,
        )
        uploaded_so_far += fsize
        if progress_cb:
            progress_cb(uploaded_so_far, total_bytes)

    return root_folder


def _list_folder(service, folder_id, trashed):
    """All files directly inside folder_id, active or trashed, paginated."""
    files, page_token = [], None
    q = f"'{folder_id}' in parents and trashed = {'true' if trashed else 'false'}"
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name)",
            pageSize=200,
            spaces="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_token,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def count_wipe_targets(folder_id):
    """How many files a wipe would touch: active in the folder + trashed from it."""
    service = build_service()
    active = _list_folder(service, folder_id, trashed=False)
    trashed = _list_folder(service, folder_id, trashed=True)
    return len(active) + len(trashed)


def wipe_folder(folder_id, progress_cb=None):
    """Permanently delete every file in folder_id, including ones already in
    the trash that originated there. Scoped to this folder only - never
    touches the account-wide trash, so files outside it are untouched.

    Runs blocking, so callers on the event loop should hand it to a thread.
    """
    service = build_service()
    targets = _list_folder(service, folder_id, trashed=False) + _list_folder(
        service, folder_id, trashed=True
    )

    deleted, errors = 0, []
    for i, f in enumerate(targets, 1):
        try:
            service.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
            deleted += 1
        except HttpError as exc:
            errors.append(f"{f.get('name', f['id'])}: {exc}")
        if progress_cb:
            progress_cb(i, len(targets))

    return {"total": len(targets), "deleted": deleted, "errors": errors}
