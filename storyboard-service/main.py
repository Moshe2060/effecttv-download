from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="EffectTV Storyboard Service")

CACHE_ROOT = Path(os.getenv("STORYBOARD_CACHE", "/var/cache/effecttv-storyboards"))
INTERVAL_SECONDS = int(os.getenv("STORYBOARD_INTERVAL_SECONDS", "10"))
MAX_CONCURRENT_JOBS = int(os.getenv("STORYBOARD_MAX_JOBS", "1"))
RETENTION_DAYS = int(os.getenv("STORYBOARD_RETENTION_DAYS", "30"))
MAX_CACHE_GB = int(os.getenv("STORYBOARD_MAX_CACHE_GB", "50"))
ALLOWED_HOSTS = {
    host.strip().lower()
    for host in os.getenv("STORYBOARD_ALLOWED_HOSTS", "effecttv.org,www.effecttv.org").split(",")
    if host.strip()
}
CACHE_ROOT.mkdir(parents=True, exist_ok=True)

jobs: dict[str, asyncio.Task] = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)


async def to_thread(function, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: function(*args))


class PrepareRequest(BaseModel):
    source: str
    cache_key: Optional[str] = None


def validate_source(source: str) -> None:
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HTTPException(400, "Invalid media URL")
    if parsed.hostname.lower() not in ALLOWED_HOSTS:
        raise HTTPException(403, "Media host is not allowed")


def storyboard_id(source: str, cache_key: Optional[str] = None) -> str:
    stable_value = cache_key.strip() if cache_key and cache_key.strip() else source
    return hashlib.sha256(stable_value.encode()).hexdigest()[:32]


def manifest_path(item_id: str) -> Path:
    return CACHE_ROOT / item_id / "manifest.json"


def read_status(item_id: str) -> dict:
    path = manifest_path(item_id)
    if path.exists():
        return json.loads(path.read_text())

    # Expose frames progressively while FFmpeg is still generating them.
    working = CACHE_ROOT / f"{item_id}.working"
    frame_count = len(list(working.glob("*.jpg"))) if working.is_dir() else 0
    return {
        "id": item_id,
        "ready": frame_count > 0,
        "complete": False,
        "interval_ms": INTERVAL_SECONDS * 1000,
        "frame_count": frame_count,
    }


def touch_storyboard(item_id: str) -> None:
    path = CACHE_ROOT / item_id
    if path.is_dir():
        now = time.time()
        os.utime(path, (now, now))


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def cleanup_cache() -> None:
    now = time.time()
    cutoff = now - RETENTION_DAYS * 86400
    directories = [path for path in CACHE_ROOT.iterdir() if path.is_dir() and not path.name.endswith(".working")]
    for path in directories:
        if path.stat().st_mtime < cutoff:
            shutil.rmtree(path, ignore_errors=True)
    directories = [path for path in CACHE_ROOT.iterdir() if path.is_dir() and not path.name.endswith(".working")]
    entries = sorted(((path.stat().st_mtime, directory_size(path), path) for path in directories), key=lambda item: item[0])
    total = sum(size for _, size, _ in entries)
    maximum = MAX_CACHE_GB * 1024 * 1024 * 1024
    for _, size, path in entries:
        if total <= maximum:
            break
        shutil.rmtree(path, ignore_errors=True)
        total -= size


async def cleanup_loop() -> None:
    while True:
        await to_thread(cleanup_cache)
        await asyncio.sleep(6 * 60 * 60)


@app.on_event("startup")
async def start_cleanup_task():
    asyncio.create_task(cleanup_loop())


def run_checked(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True, timeout=7200)
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-2000:])
    return result.stdout.strip()


async def generate_storyboard(item_id: str, source: str) -> None:
    async with semaphore:
        target = CACHE_ROOT / item_id
        temp = CACHE_ROOT / f"{item_id}.working"
        try:
            if manifest_path(item_id).exists():
                return
            shutil.rmtree(temp, ignore_errors=True)
            temp.mkdir(parents=True)
            duration_raw = await to_thread(
                run_checked,
                ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", source],
            )
            duration = max(0.0, float(duration_raw))
            await to_thread(
                run_checked,
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-i", source,
                    "-vf", f"fps=1/{INTERVAL_SECONDS},scale=320:-2",
                    "-q:v", "5", "-vsync", "vfr", str(temp / "%06d.jpg"),
                ],
            )
            frame_count = len(list(temp.glob("*.jpg")))
            manifest = {
                "id": item_id,
                "ready": frame_count > 0,
                "interval_ms": INTERVAL_SECONDS * 1000,
                "frame_count": frame_count,
                "duration_ms": int(duration * 1000),
            }
            (temp / "manifest.json").write_text(json.dumps(manifest))
            shutil.rmtree(target, ignore_errors=True)
            temp.rename(target)
        except Exception as exc:
            shutil.rmtree(temp, ignore_errors=True)
            (CACHE_ROOT / f"{item_id}.error").write_text(str(exc))
        finally:
            jobs.pop(item_id, None)


@app.post("/prepare")
async def prepare(request: PrepareRequest):
    validate_source(request.source)
    item_id = storyboard_id(request.source, request.cache_key)
    status = read_status(item_id)
    if status["ready"]:
        touch_storyboard(item_id)
        return status
    if item_id not in jobs:
        jobs[item_id] = asyncio.create_task(generate_storyboard(item_id, request.source))
    return status


@app.get("/status/{item_id}")
async def status(item_id: str):
    if len(item_id) != 32 or any(c not in "0123456789abcdef" for c in item_id):
        raise HTTPException(400, "Invalid storyboard id")
    return read_status(item_id)


@app.get("/files/{item_id}/{filename}")
async def frame(item_id: str, filename: str):
    if len(item_id) != 32 or not filename.endswith(".jpg") or not filename[:-4].isdigit():
        raise HTTPException(400, "Invalid frame path")
    path = CACHE_ROOT / item_id / filename
    if not path.exists():
        path = CACHE_ROOT / f"{item_id}.working" / filename
    if not path.exists():
        raise HTTPException(404, "Frame not ready")
    touch_storyboard(item_id)
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=2592000, immutable"})
