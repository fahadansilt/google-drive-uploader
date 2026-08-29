"""BitTorrent & Magnet download engine powered by aria2c JSON-RPC."""
import asyncio
import base64
import logging
import os
import shutil
import subprocess
import time
from typing import Callable, Optional, Dict, Any, List

import aiohttp

import config

log = logging.getLogger("torrent")


class Aria2NotInstalledError(RuntimeError):
    """Raised when aria2c executable is not installed on the system."""


class TransferCancelled(Exception):
    """Raised when a transfer is cancelled by user request."""


class TorrentManager:
    """Manages an aria2c daemon and interacts with its JSON-RPC interface."""

    def __init__(
        self,
        host: str = config.ARIA2_HOST,
        port: int = config.ARIA2_PORT,
        secret: str = config.ARIA2_SECRET,
    ):
        self.host = host.rstrip("/")
        self.port = port
        self.secret = secret
        self.rpc_url = f"{self.host}:{self.port}/jsonrpc"
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._req_id = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._proc:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=3.0)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None

    async def ensure_running(self):
        """Ensure aria2c daemon is running and reachable."""
        async with self._lock:
            # Test if already responding
            if await self._ping():
                return

            # Check if aria2c binary exists in PATH
            aria2_bin = shutil.which("aria2c")
            if not aria2_bin:
                raise Aria2NotInstalledError(
                    "aria2c is not installed on this system. "
                    "Install it via:\n"
                    "• Linux (Ubuntu/Debian): sudo apt update && sudo apt install -y aria2\n"
                    "• Windows: winget install aria2 OR choco install aria2"
                )

            # Spawn aria2c daemon
            cmd = [
                aria2_bin,
                "--enable-rpc=true",
                "--rpc-listen-all=false",
                f"--rpc-listen-port={self.port}",
                "--rpc-max-request-size=64M",
                f"--dir={os.path.abspath(config.DOWNLOAD_DIR)}",
                "--seed-time=0",
                "--max-upload-limit=1K",
                "--bt-stop-timeout=600",
                "--summary-interval=0",
                "--quiet=true",
                "--enable-dht=true",
                "--enable-peer-exchange=true",
                "--bt-max-peers=60",
                "--follow-torrent=mem",
            ]
            if self.secret:
                cmd.append(f"--rpc-secret={self.secret}")

            log.info("Starting aria2c daemon on port %s...", self.port)
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )

            # Wait up to 5 seconds for RPC to become ready
            for _ in range(25):
                await asyncio.sleep(0.2)
                if await self._ping():
                    log.info("aria2c daemon started and responding on port %s", self.port)
                    return

            raise RuntimeError("aria2c daemon started but did not respond to RPC on port " + str(self.port))

    async def _ping(self) -> bool:
        try:
            session = await self._get_session()
            async with session.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": "ping",
                    "method": "aria2.getVersion",
                    "params": [f"token:{self.secret}"] if self.secret else [],
                },
                timeout=aiohttp.ClientTimeout(total=1.5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return "result" in data
        except Exception:
            return False
        return False

    async def _call(self, method: str, params: Optional[list] = None) -> Any:
        session = await self._get_session()
        self._req_id += 1
        rpc_params = []
        if self.secret:
            rpc_params.append(f"token:{self.secret}")
        if params:
            rpc_params.extend(params)

        payload = {
            "jsonrpc": "2.0",
            "id": f"req-{self._req_id}",
            "method": method,
            "params": rpc_params,
        }

        async with session.post(
            self.rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10.0),
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"aria2 RPC HTTP {resp.status}: {text}")
            data = await resp.json()
            if "error" in data:
                err = data["error"]
                raise RuntimeError(f"aria2 RPC Error {err.get('code')}: {err.get('message')}")
            return data.get("result")

    async def add_magnet(self, magnet_uri: str, options: Optional[dict] = None) -> str:
        await self.ensure_running()
        opts = options or {}
        gid = await self._call("aria2.addUri", [[magnet_uri], opts])
        return gid

    async def add_torrent_file(self, torrent_path: str, options: Optional[dict] = None) -> str:
        await self.ensure_running()
        with open(torrent_path, "rb") as f:
            b64_content = base64.b64encode(f.read()).decode("utf-8")
        opts = options or {}
        gid = await self._call("aria2.addTorrent", [b64_content, [], opts])
        return gid

    async def tell_status(self, gid: str) -> Dict[str, Any]:
        return await self._call("aria2.tellStatus", [gid])

    async def force_remove(self, gid: str):
        try:
            await self._call("aria2.forceRemove", [gid])
        except Exception as exc:
            log.debug("force_remove on %s ignored: %s", gid, exc)
        try:
            await self._call("aria2.removeDownloadResult", [gid])
        except Exception:
            pass

    async def download(
        self,
        magnet_or_path: str,
        download_dir: str,
        progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
        cancel_event: Optional[asyncio.Event] = None,
    ) -> Dict[str, Any]:
        """Download a magnet link or .torrent file, tracking progress and supporting cancellation.

        Returns a dictionary describing the completed download:
        {
            "name": str,
            "is_dir": bool,
            "path": str,
            "total_size": int,
            "files": list[str]
        }
        """
        await self.ensure_running()
        target_dir = os.path.abspath(download_dir)
        os.makedirs(target_dir, exist_ok=True)

        is_torrent_file = os.path.exists(magnet_or_path) and magnet_or_path.endswith(".torrent")
        opts = {"dir": target_dir}

        if is_torrent_file:
            gid = await self.add_torrent_file(magnet_or_path, opts)
        else:
            gid = await self.add_magnet(magnet_or_path, opts)

        log.info("Started aria2 torrent download with GID %s", gid)
        seen_gids = [gid]

        try:
            while True:
                if cancel_event and cancel_event.is_set():
                    log.info("Download cancelled by user for GID %s", gid)
                    for g in seen_gids:
                        await self.force_remove(g)
                    raise TransferCancelled("Torrent download cancelled by user.")

                status_info = await self.tell_status(gid)
                status = status_info.get("status")

                # Handle magnet metadata resolution (followedBy GID chain)
                followed_by = status_info.get("followedBy")
                if followed_by and len(followed_by) > 0:
                    new_gid = followed_by[0]
                    if new_gid != gid:
                        log.info("Magnet metadata resolved, switching GID %s -> %s", gid, new_gid)
                        gid = new_gid
                        seen_gids.append(gid)
                        continue

                if status == "complete":
                    log.info("Torrent download complete for GID %s", gid)
                    try:
                        await self._call("aria2.removeDownloadResult", [gid])
                    except Exception:
                        pass
                    return self._parse_completed(status_info, target_dir)

                if status == "error":
                    err_msg = status_info.get("errorMessage", "Unknown aria2 error")
                    err_code = status_info.get("errorCode", "Unknown")
                    raise RuntimeError(f"Torrent download failed ({err_code}): {err_msg}")

                if status == "removed":
                    raise TransferCancelled("Torrent download was removed.")

                # Extract live metrics for progress callback
                if progress_cb:
                    completed_len = int(status_info.get("completedLength", 0))
                    total_len = int(status_info.get("totalLength", 0))
                    dl_speed = int(status_info.get("downloadSpeed", 0))
                    num_seeders = int(status_info.get("numSeeders", 0))
                    connections = int(status_info.get("connections", 0))
                    peers = max(connections - num_seeders, 0) if num_seeders > 0 else connections

                    bt_info = status_info.get("bittorrent", {}).get("info", {})
                    name = bt_info.get("name")
                    if not name:
                        files = status_info.get("files", [])
                        if files and files[0].get("path"):
                            name = os.path.basename(files[0]["path"])
                    if not name:
                        name = "Resolving magnet metadata..."

                    is_metadata = total_len == 0 or status_info.get("followedBy") is not None

                    progress_cb({
                        "name": name,
                        "done": completed_len,
                        "total": total_len,
                        "speed": dl_speed,
                        "seeds": num_seeders,
                        "peers": peers,
                        "stage": "fetching metadata" if is_metadata else "downloading (torrent)",
                    })

                await asyncio.sleep(1.0)

        except Exception:
            for g in seen_gids:
                try:
                    await self.force_remove(g)
                except Exception:
                    pass
            raise

    def _parse_completed(self, status_info: Dict[str, Any], target_dir: str) -> Dict[str, Any]:
        bt_info = status_info.get("bittorrent", {}).get("info", {})
        torrent_name = bt_info.get("name")
        files = status_info.get("files", [])
        total_size = int(status_info.get("totalLength", 0))

        real_paths = [f["path"] for f in files if f.get("path") and os.path.exists(f["path"])]

        if not real_paths:
            if torrent_name and os.path.exists(os.path.join(target_dir, torrent_name)):
                real_paths = [os.path.join(target_dir, torrent_name)]
            else:
                raise RuntimeError("Torrent completed but downloaded files could not be located.")

        if not torrent_name:
            torrent_name = os.path.basename(real_paths[0])

        possible_dir = os.path.join(target_dir, torrent_name)
        if os.path.isdir(possible_dir):
            return {
                "name": torrent_name,
                "is_dir": True,
                "path": possible_dir,
                "total_size": total_size,
                "files": real_paths,
            }

        primary_file = real_paths[0]
        return {
            "name": torrent_name,
            "is_dir": os.path.isdir(primary_file),
            "path": primary_file,
            "total_size": total_size,
            "files": real_paths,
        }


# Global singleton instance
torrent_manager = TorrentManager()
