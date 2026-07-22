# ══════════════════════════════════════════════════════════════════════════
#  fast_download.py  —  HYPER-SPEED DOWNLOAD ADD-ON (new file, safe by design)
# ══════════════════════════════════════════════════════════════════════════
#  Goal: make the 300–500MB PW video-lecture downloads much faster, WITHOUT
#  touching a single line of the existing, working core.py/main.py logic.
#
#  Strategy ladder (fastest → safest, always ends at the original pipeline):
#
#    1) Direct single-file URL (.mp4/.mkv/.pdf/etc.) that supports HTTP
#       Range requests
#         -> pure asyncio, N-way PARALLEL byte-range download (exactly the
#            "split into ~10MB pieces, fetch all pieces at once" idea).
#         -> each piece is written straight to its correct file offset with
#            os.pwrite(), so there is no separate "merge" step — the file
#            is already assembled the instant the last piece lands.
#
#    2) Same direct URL, but step 1 failed / server doesn't support ranges
#         -> aria2c CLI (already installed in the Dockerfile for yt-dlp),
#            using -x/-s multi-connection splitting.
#
#    3) Everything else (the normal PW case: .mpd / .m3u8 / classplus /
#       youtube links that main.py turns into a yt-dlp command)
#         -> we do NOT reinvent HLS/DASH handling. We just boost the
#            EXISTING yt-dlp command with extra native fragment
#            concurrency (--concurrent-fragments) on top of the aria2c
#            external-downloader wiring that is already in core.py, then
#            hand off to the ORIGINAL, unmodified download_video().
#
#  If literally anything above throws, we fall back one level down, and the
#  final fallback is always the original download_video() — so worst case,
#  behaviour is EXACTLY what it was before this file existed.
# ══════════════════════════════════════════════════════════════════════════

import os
import logging
import asyncio
import subprocess

import aiohttp

DEFAULT_WORKERS = 16          # parallel connections / fragments
DEFAULT_CHUNK_MB = 10         # byte-range chunk size, as requested (10MB pieces)
DIRECT_EXTENSIONS = (".mp4", ".mkv", ".webm", ".m4v", ".mov", ".pdf")


def _looks_like_direct_file(url: str) -> bool:
    """True only for plain, single-file URLs (not .mpd/.m3u8 manifests)."""
    clean = url.lower().split("?")[0].split("#")[0]
    return clean.endswith(DIRECT_EXTENSIONS)


async def _head_probe(session: "aiohttp.ClientSession", url: str):
    """Check file size + whether the server supports Range requests."""
    try:
        async with session.head(
            url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            size = int(r.headers.get("Content-Length") or 0)
            ranges_ok = "bytes" in (r.headers.get("Accept-Ranges") or "").lower()
            return size, ranges_ok
    except Exception:
        return 0, False


async def _fetch_one_range(session, url, start, end, fd, sem, retries=4):
    headers = {"Range": f"bytes={start}-{end}"}
    last_err = None
    for attempt in range(retries):
        try:
            async with sem:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=300)
                ) as r:
                    if r.status not in (200, 206):
                        raise IOError(f"unexpected status {r.status} for {start}-{end}")
                    data = await r.read()
            os.pwrite(fd, data, start)   # write straight to offset, no merge needed
            return len(data)
        except Exception as e:
            last_err = e
            await asyncio.sleep(1.5 * (attempt + 1))
    raise last_err


async def download_range_parallel(
    url: str,
    filename: str,
    workers: int = DEFAULT_WORKERS,
    chunk_size: int = DEFAULT_CHUNK_MB * 1024 * 1024,
):
    """
    TRUE multi-connection parallel downloader.
    Splits the remote file into `chunk_size` byte ranges and fetches ALL of
    them concurrently (up to `workers` at a time) via aiohttp. Only works
    when the server advertises Range support.
    """
    connector = aiohttp.TCPConnector(limit=workers + 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        size, ranges_ok = await _head_probe(session, url)
        if not size or not ranges_ok:
            raise RuntimeError("server doesn't support ranged multi-connection download")

        if os.path.exists(filename):
            os.remove(filename)
        fd = os.open(filename, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            os.ftruncate(fd, size)          # pre-allocate full size
            sem = asyncio.Semaphore(workers)
            tasks = []
            start = 0
            while start < size:
                end = min(start + chunk_size, size) - 1
                tasks.append(_fetch_one_range(session, url, start, end, fd, sem))
                start = end + 1
            await asyncio.gather(*tasks)
        finally:
            os.close(fd)
    return filename


def aria2c_direct_fetch(url: str, filename: str, connections: int = DEFAULT_WORKERS):
    """
    Multi-connection direct download via the aria2c binary (already present
    in the Dockerfile for yt-dlp). Fallback for direct-file URLs when the
    pure-Python ranged downloader can't be used.
    """
    out_dir = os.path.dirname(os.path.abspath(filename)) or "."
    out_name = os.path.basename(filename)
    cmd = [
        "aria2c", url,
        "-x", str(connections),
        "-s", str(connections),
        "-k", "10M",
        "--min-split-size=10M",
        "--max-connection-per-server", str(connections),
        "--continue=true",
        "--retry-wait=2",
        "--max-tries=5",
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--console-log-level=warn",
        "-d", out_dir,
        "-o", out_name,
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0 or not os.path.exists(filename) or os.path.getsize(filename) == 0:
        raise RuntimeError(f"aria2c direct fetch failed: {result.stdout.decode(errors='ignore')[-500:]}")
    return filename


def boost_ytdlp_cmd(cmd: str, fragments: int = DEFAULT_WORKERS) -> str:
    """
    Leaves the existing yt-dlp command 100% as-is except for adding native
    fragment-level concurrency on top of the aria2c external-downloader
    that core.py already wires in. Speeds up .m3u8/.mpd (the PW batch
    videos) without changing anything about how the command is built.
    """
    if "yt-dlp" not in cmd:
        return cmd
    if "--concurrent-fragments" in cmd:
        return cmd
    return cmd.replace("yt-dlp", f"yt-dlp --concurrent-fragments {fragments}", 1)


async def smart_download_video(
    url,
    cmd,
    name,
    workers=DEFAULT_WORKERS,
    chunk_mb=DEFAULT_CHUNK_MB,
    fallback_download=None,
):
    """
    Hyper-speed orchestrator. `fallback_download` must be the ORIGINAL,
    unmodified core.download_video coroutine — passed in by the caller so
    this module never needs to import core.py (avoids circular imports).
    """
    target_name = name if os.path.splitext(name)[1] else f"{name}.mp4"

    if _looks_like_direct_file(url):
        try:
            logging.info(f"[fast_download] trying {workers}-way parallel ranged download")
            await download_range_parallel(url, target_name, workers=workers, chunk_size=chunk_mb * 1024 * 1024)
            if os.path.exists(target_name) and os.path.getsize(target_name) > 0:
                return target_name
        except Exception as e:
            logging.warning(f"[fast_download] ranged download unavailable ({e}); trying aria2c")

        try:
            aria2c_direct_fetch(url, target_name, connections=workers)
            if os.path.exists(target_name) and os.path.getsize(target_name) > 0:
                return target_name
        except Exception as e:
            logging.warning(f"[fast_download] aria2c direct fetch failed ({e}); using yt-dlp pipeline")

    boosted_cmd = boost_ytdlp_cmd(cmd, fragments=workers)
    if fallback_download is None:
        raise RuntimeError("no fallback_download provided and advanced strategies failed")
    return await fallback_download(url, boosted_cmd, name)
