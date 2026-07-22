# ══════════════════════════════════════════════════════════════════════════
#  fast_upload.py  —  HYPER-SPEED UPLOAD ADD-ON (new file, safe by design)
# ══════════════════════════════════════════════════════════════════════════
#  Pyrogram's default upload sends one file-part (512KB) at a time and
#  waits for Telegram's reply before sending the next one. That's the main
#  upload bottleneck for the 300-500MB video lectures.
#
#  This module uploads a file by firing MANY SaveFilePart/SaveBigFilePart
#  requests CONCURRENTLY on the same media session (instead of one-by-one),
#  then sends the finished file via a raw messages.SendMedia call.
#
#  This touches Pyrogram's raw/session internals, which can vary slightly
#  between Pyrogram versions/forks. Because of that, EVERY entry point here
#  is meant to be wrapped in try/except by the caller (core.py does this) —
#  if anything about this fails, the caller falls back to the plain,
#  original m.reply_video()/m.reply_document() and the bot behaves exactly
#  as before. Nothing here can break the existing upload path.
# ══════════════════════════════════════════════════════════════════════════

import os
import math
import asyncio
import logging
from hashlib import md5

try:
    from pyrogram import raw
    from pyrogram.session import Session
    _RAW_OK = True
except Exception as _e:
    _RAW_OK = False
    logging.info(f"[fast_upload] pyrogram raw API not importable, turbo upload disabled: {_e}")

PART_SIZE = 512 * 1024  # Telegram's fixed upload part size


async def fast_upload(client, path: str, workers: int = 12, progress=None, progress_args=()):
    """
    Uploads `path` using many concurrent part-upload RPCs instead of
    Pyrogram's default sequential one-part-at-a-time loop.
    Returns a raw InputFile/InputFileBig ready for InputMediaUploadedDocument.
    Raises on any problem so the caller can fall back safely.
    """
    if not _RAW_OK:
        raise RuntimeError("pyrogram raw API unavailable")

    file_size = os.path.getsize(path)
    if file_size <= 0:
        raise RuntimeError("empty file, nothing to upload")

    is_big = file_size > 10 * 1024 * 1024
    file_total_parts = math.ceil(file_size / PART_SIZE)
    file_id = client.rnd_id()

    session = Session(
        client,
        await client.storage.dc_id(),
        await client.storage.auth_key(),
        await client.storage.test_mode(),
        is_media=True,
    )
    await session.start()

    uploaded_bytes = 0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(workers)
    md5_sum = md5() if not is_big else None

    async def push_part(index, data):
        nonlocal uploaded_bytes
        async with sem:
            if is_big:
                rpc = raw.functions.upload.SaveBigFilePart(
                    file_id=file_id, file_part=index,
                    file_total_parts=file_total_parts, bytes=data,
                )
            else:
                rpc = raw.functions.upload.SaveFilePart(
                    file_id=file_id, file_part=index, bytes=data,
                )
            ok = await session.invoke(rpc)
            if not ok:
                raise RuntimeError(f"Telegram rejected part {index}")
        async with lock:
            uploaded_bytes += len(data)
            if progress:
                try:
                    await progress(uploaded_bytes, file_size, *progress_args)
                except Exception:
                    pass

    try:
        tasks = []
        idx = 0
        with open(path, "rb") as f:
            while True:
                data = f.read(PART_SIZE)
                if not data:
                    break
                if md5_sum is not None:
                    md5_sum.update(data)   # sequential read order -> checksum stays correct
                tasks.append(asyncio.create_task(push_part(idx, data)))
                idx += 1
        await asyncio.gather(*tasks)
    finally:
        await session.stop()

    name = os.path.basename(path)
    if is_big:
        return raw.types.InputFileBig(id=file_id, parts=file_total_parts, name=name)
    return raw.types.InputFile(
        id=file_id, parts=file_total_parts, name=name,
        md5_checksum=md5_sum.hexdigest() if md5_sum else "",
    )


async def turbo_send_video(
    client, chat_id, filepath, caption, thumb_path,
    duration, width=1280, height=720,
    workers: int = 12, progress=None, progress_args=(),
):
    """
    Full turbo replacement for client.send_video():
    fast_upload() for the big file + normal save_file() for the (tiny)
    thumbnail + a manual raw messages.SendMedia call. Raises on any
    problem so the caller can fall back to the original send_vid().
    """
    if not _RAW_OK:
        raise RuntimeError("pyrogram raw API unavailable")

    peer = await client.resolve_peer(chat_id)

    thumb_input = None
    if thumb_path and os.path.exists(thumb_path):
        try:
            thumb_input = await client.save_file(thumb_path)  # small file, normal path is fine
        except Exception:
            thumb_input = None

    big_input = await fast_upload(client, filepath, workers=workers, progress=progress, progress_args=progress_args)

    parsed = await client.parser.parse(caption or "")
    message_text = parsed["message"]
    entities = parsed["entities"]

    attributes = [
        raw.types.DocumentAttributeVideo(
            supports_streaming=True,
            duration=int(duration or 0),
            w=int(width or 1280),
            h=int(height or 720),
        ),
        raw.types.DocumentAttributeFilename(file_name=os.path.basename(filepath)),
    ]

    media = raw.types.InputMediaUploadedDocument(
        file=big_input,
        thumb=thumb_input,
        mime_type="video/mp4",
        attributes=attributes,
    )

    return await client.invoke(
        raw.functions.messages.SendMedia(
            peer=peer,
            media=media,
            message=message_text,
            random_id=client.rnd_id(),
            entities=entities,
        )
    )
