"""
Backfill script — processes existing channel history.
  python backfill.py            # all messages
  python backfill.py --limit 200 --offset 0
"""
import os, asyncio, argparse, logging
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

from main import (bot, DB_CHANNEL, ensure_indexes, ensure_superadmin,
                  parse_caption, ai_classify, tmdb_search, tmdb_detail,
                  tmdb_episode, save_movie, save_episode, write_audit,
                  get_file_id, get_filename, get_size_mb, col)
import httpx
from datetime import datetime, timezone

async def backfill(limit=0, offset=0):
    await ensure_indexes()
    await ensure_superadmin()
    await bot.start()
    log.info(f"Backfill start — limit={limit or 'all'}, offset={offset}")
    count = 0
    async with httpx.AsyncClient() as http:
        async for msg in bot.get_chat_history(DB_CHANNEL, limit=limit or 0, offset=offset):
            if not any([msg.video, msg.document, msg.audio]):
                continue
            file_id  = get_file_id(msg)
            filename = get_filename(msg)
            caption  = (msg.caption or "").strip()
            size_mb  = get_size_mb(msg)
            if not file_id:
                continue
            if await col["bot_logs"].find_one({"msg_id": msg.id}):
                log.info(f"Skip already processed msg {msg.id}")
                continue
            info = parse_caption(caption)
            if not info:
                info = await ai_classify(filename, caption)
            kind = info.get("type","unknown")
            if kind == "unknown" or not info.get("title"):
                continue
            tmdb_kind   = "movie" if kind == "movie" else "tv"
            tmdb_result = await tmdb_search(http, info["title"], info.get("year"), tmdb_kind)
            tmdb_id     = tmdb_result["id"] if tmdb_result else None
            detail      = await tmdb_detail(http, tmdb_id, tmdb_kind) if tmdb_id else {}
            if kind == "movie":
                slug, action = await save_movie(info, detail, file_id, msg.id, size_mb)
                coll = "movies"
            else:
                ep_d = {}
                if tmdb_id and info.get("season") and info.get("episode"):
                    ep_d = await tmdb_episode(http, tmdb_id, info["season"], info["episode"])
                slug, action = await save_episode(info, detail, ep_d, file_id, msg.id, size_mb)
                coll = "series"
            await col["bot_logs"].insert_one({
                "msg_id":msg.id,"filename":filename,"caption":caption[:300],
                "ai_info":info,"slug":slug,"action":action,
                "timestamp":datetime.now(timezone.utc),"source":"backfill"
            })
            await write_audit(action, coll, slug, {"msg_id":msg.id,"quality":info.get("quality"),"filename":filename})
            count += 1
            log.info(f"[{count}] {action}: {slug}")
            await asyncio.sleep(0.4)
    await bot.stop()
    log.info(f"Backfill done — {count} files processed")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit",  type=int, default=0)
    p.add_argument("--offset", type=int, default=0)
    a = p.parse_args()
    asyncio.run(backfill(a.limit, a.offset))
