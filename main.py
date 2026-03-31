"""
SC Files Bot v4
===============
Watches Telegram DB channel. Caption format (structured):
  Title: Kaithi
  Year: 2019
  Quality: 1080p
  Language: Tamil
  Type: Movie
  Season: -
  Episode: -
  Extras: PreDVD

For each new file:
  1. Parse structured caption (or fall back to AI)
  2. TMDB lookup for rich metadata
  3. Save/upsert into MongoDB
  4. Log to admin log channel + MongoDB audit_logs
"""
import os, re, json, asyncio, logging, hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

from pyrogram import Client, filters, idle
from pyrogram.types import Message
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
import anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
API_ID        = int(os.environ["TELEGRAM_API_ID"])
API_HASH      = os.environ["TELEGRAM_API_HASH"]
BOT_TOKEN     = os.environ["TELEGRAM_BOT_TOKEN"]
DB_CHANNEL    = int(os.environ["DB_CHANNEL_ID"])
LOG_CHANNEL   = int(os.environ.get("LOG_CHANNEL_ID", "0") or "0")
TMDB_KEY      = os.environ["TMDB_API_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MONGODB_URI   = os.environ["MONGODB_URI"]
TMDB_BASE     = "https://api.themoviedb.org/3"

# ── Clients ───────────────────────────────────────────────────
bot   = Client("scfiles_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
ai    = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
mongo = AsyncIOMotorClient(MONGODB_URI)
db    = mongo["scfiles"]

col = {
    "movies":       db["movies"],
    "series":       db["series"],
    "collections":  db["collections"],
    "bot_logs":     db["bot_logs"],
    "audit_logs":   db["audit_logs"],
    "users":        db["users"],
}

# ── Helpers ───────────────────────────────────────────────────
def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")

def get_file_id(msg: Message) -> str | None:
    for attr in ("video", "document", "audio"):
        m = getattr(msg, attr, None)
        if m:
            return m.file_id
    return None

def get_filename(msg: Message) -> str:
    for attr in ("video", "document", "audio"):
        m = getattr(msg, attr, None)
        if m and hasattr(m, "file_name") and m.file_name:
            return m.file_name
    return ""

def get_size_mb(msg: Message) -> float:
    for attr in ("video", "document", "audio"):
        m = getattr(msg, attr, None)
        if m and hasattr(m, "file_size") and m.file_size:
            return round(m.file_size / (1024 * 1024), 1)
    return 0.0

# ── Caption parser ─────────────────────────────────────────────
CAPTION_FIELDS = {
    "title":    r"(?:title|name)\s*[:\-]\s*(.+)",
    "year":     r"year\s*[:\-]\s*(\d{4})",
    "quality":  r"quality\s*[:\-]\s*(\d{3,4})",
    "language": r"language\s*[:\-]\s*(.+)",
    "type":     r"type\s*[:\-]\s*(.+)",
    "season":   r"season\s*[:\-]\s*(\d+)",
    "episode":  r"episode\s*[:\-]\s*(\d+)",
    "extras":   r"extras?\s*[:\-]\s*(.+)",
}
LANG_ISO = {
    "tamil":"ta","hindi":"hi","malayalam":"ml","telugu":"te",
    "kannada":"kn","english":"en","french":"fr","spanish":"es",
}

def parse_caption(caption: str) -> dict | None:
    """Parse structured caption. Returns dict or None if not structured."""
    if not caption or ":" not in caption:
        return None
    result = {}
    for field, pattern in CAPTION_FIELDS.items():
        m = re.search(pattern, caption, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val and val not in ("-", "—", "N/A", "n/a"):
                result[field] = val
    if not result.get("title"):
        return None  # Not structured enough
    # Normalise
    if result.get("year"):
        try: result["year"] = int(result["year"])
        except: result.pop("year", None)
    if result.get("quality"):
        result["quality"] = re.sub(r"[pP]", "", result["quality"])
    if result.get("language"):
        lang_raw = result["language"].lower().strip()
        result["language"] = LANG_ISO.get(lang_raw, lang_raw[:2])
    media_type_raw = result.get("type", "movie").lower()
    if "series" in media_type_raw or "episode" in media_type_raw:
        result["type"] = "series_episode"
    elif "movie" in media_type_raw or "film" in media_type_raw:
        result["type"] = "movie"
    else:
        result["type"] = "movie"
    if result.get("season"):
        try: result["season"] = int(result["season"])
        except: result.pop("season", None)
    if result.get("episode"):
        try: result["episode"] = int(result["episode"])
        except: result.pop("episode", None)
    result["confidence"] = 1.0
    return result

# ── AI fallback ───────────────────────────────────────────────
AI_PROMPT = """You are a media file classifier. Given a Telegram filename and caption, return ONLY valid JSON:
{
  "type": "movie" | "series_episode" | "unknown",
  "title": "Clean English title",
  "year": 2024 | null,
  "language": "ta" | "hi" | "ml" | "te" | "kn" | "en" | "other",
  "quality": "480" | "720" | "1080" | "2160" | "unknown",
  "release_status": "predvd" | "dvdrip" | "webrip" | "webdl" | "bluray" | "unknown",
  "season": 1 | null,
  "episode": 1 | null,
  "extras": "short note or null",
  "confidence": 0.0-1.0
}

Filename: {filename}
Caption: {caption}"""

async def ai_classify(filename: str, caption: str) -> dict:
    try:
        resp = ai.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role":"user","content":AI_PROMPT.format(filename=filename or "(none)", caption=caption or "(none)")}]
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*","",raw); raw = re.sub(r"\s*```$","",raw)
        return json.loads(raw)
    except Exception as e:
        log.warning(f"AI classify failed: {e}")
        return {"type":"unknown","title":filename,"confidence":0.0}

# ── TMDB ──────────────────────────────────────────────────────
async def tmdb_search(http: httpx.AsyncClient, title: str, year: int | None, kind: str) -> dict | None:
    ep = "/search/movie" if kind == "movie" else "/search/tv"
    p  = {"api_key": TMDB_KEY, "query": title, "language": "en-US"}
    if year: p["year" if kind=="movie" else "first_air_date_year"] = year
    try:
        r = await http.get(f"{TMDB_BASE}{ep}", params=p, timeout=10)
        results = r.json().get("results", [])
        return results[0] if results else None
    except Exception as e:
        log.warning(f"TMDB search: {e}"); return None

async def tmdb_detail(http: httpx.AsyncClient, tmdb_id: int, kind: str) -> dict:
    append = "credits" if kind == "movie" else "aggregate_credits"
    try:
        r = await http.get(
            f"{TMDB_BASE}/{'movie' if kind=='movie' else 'tv'}/{tmdb_id}",
            params={"api_key":TMDB_KEY,"language":"en-US","append_to_response":append}, timeout=10
        )
        return r.json()
    except Exception as e:
        log.warning(f"TMDB detail: {e}"); return {}

async def tmdb_episode(http: httpx.AsyncClient, tmdb_id: int, s: int, e: int) -> dict:
    try:
        r = await http.get(f"{TMDB_BASE}/tv/{tmdb_id}/season/{s}/episode/{e}",
                           params={"api_key":TMDB_KEY,"language":"en-US"}, timeout=10)
        return r.json()
    except: return {}

# ── MongoDB saves ──────────────────────────────────────────────
async def save_movie(info: dict, detail: dict, file_id: str, msg_id: int, size_mb: float) -> tuple[str, str]:
    quality = info.get("quality","unknown")
    extras  = info.get("extras") or info.get("release_status")
    title   = detail.get("title") or info.get("title","")
    year    = info.get("year") or (int(detail["release_date"][:4]) if detail.get("release_date") else None)
    slug    = slugify(f"{title}-{year}" if year else title)
    if extras and extras not in ("unknown","webdl","webrip"):
        slug = slugify(f"{title}-{extras}-{year}" if year else f"{title}-{extras}")

    dl_entry = {"file_id": file_id, "size_mb": size_mb, "msg_id": msg_id, "added_at": datetime.now(timezone.utc)}
    existing = await col["movies"].find_one({"id": slug})

    if existing:
        await col["movies"].update_one({"id":slug}, {"$set":{
            f"downloads.{quality}": dl_entry,
            "updated_at": datetime.now(timezone.utc)
        }})
        return slug, "updated"

    cast_src = detail.get("credits",{}).get("cast",[]) or []
    doc = {
        "id":            slug,
        "tmdb_id":       detail.get("id"),
        "title":         title,
        "original_title":detail.get("original_title"),
        "year":          year,
        "language":      info.get("language","unknown"),
        "genres":        [g["name"] for g in detail.get("genres",[])],
        "rating":        round(detail.get("vote_average",0), 1),
        "vote_count":    detail.get("vote_count",0),
        "overview":      detail.get("overview",""),
        "poster_path":   detail.get("poster_path"),
        "backdrop_path": detail.get("backdrop_path"),
        "runtime":       detail.get("runtime"),
        "cast":          [{"name":c["name"],"character":c.get("character",""),"profile":c.get("profile_path")}
                          for c in cast_src[:10]],
        "extras":        extras,
        "release_status":info.get("release_status","unknown"),
        "downloads":     {quality: dl_entry},
        "subtitles":     {},
        "added_at":      datetime.now(timezone.utc),
        "updated_at":    datetime.now(timezone.utc),
    }
    await col["movies"].insert_one(doc)
    return slug, "inserted"

async def save_episode(info: dict, sv_detail: dict, ep_detail: dict,
                       file_id: str, msg_id: int, size_mb: float) -> tuple[str, str]:
    quality = info.get("quality","unknown")
    title   = sv_detail.get("name") or info.get("title","")
    slug    = slugify(title)
    snum    = info.get("season",1) or 1
    enum    = info.get("episode",1) or 1

    ep_doc = {
        "ep_number":  enum,
        "name":       ep_detail.get("name", f"Episode {enum}"),
        "overview":   ep_detail.get("overview",""),
        "still_path": ep_detail.get("still_path"),
        "links":      {quality: {"file_id":file_id,"size_mb":size_mb,"msg_id":msg_id}},
        "subtitle":   "",
        "added_at":   datetime.now(timezone.utc),
    }

    existing = await col["series"].find_one({"id": slug})
    if existing:
        seasons = existing.get("seasons", [])
        s_idx   = next((i for i,s in enumerate(seasons) if s["season_number"]==snum), None)
        if s_idx is not None:
            eps   = seasons[s_idx].get("episodes",[])
            e_idx = next((i for i,e in enumerate(eps) if e["ep_number"]==enum), None)
            if e_idx is not None:
                await col["series"].update_one({"id":slug},{"$set":{
                    f"seasons.{s_idx}.episodes.{e_idx}.links.{quality}": ep_doc["links"][quality],
                    "updated_at": datetime.now(timezone.utc)
                }})
            else:
                await col["series"].update_one({"id":slug},{"$push":{f"seasons.{s_idx}.episodes":ep_doc},"$set":{"updated_at":datetime.now(timezone.utc)}})
        else:
            await col["series"].update_one({"id":slug},{"$push":{"seasons":{"season_number":snum,"poster_path":None,"episodes":[ep_doc]}},"$set":{"updated_at":datetime.now(timezone.utc)}})
        return slug, "updated"

    cast_src = sv_detail.get("aggregate_credits",{}).get("cast",[]) or []
    doc = {
        "id":            slug,
        "tmdb_id":       sv_detail.get("id"),
        "title":         title,
        "original_title":sv_detail.get("original_name"),
        "language":      info.get("language","unknown"),
        "genres":        [g["name"] for g in sv_detail.get("genres",[])],
        "rating":        round(sv_detail.get("vote_average",0),1),
        "overview":      sv_detail.get("overview",""),
        "poster_path":   sv_detail.get("poster_path"),
        "backdrop_path": sv_detail.get("backdrop_path"),
        "cast":          [{"name":c["name"],"profile":c.get("profile_path")} for c in cast_src[:10]],
        "seasons":       [{"season_number":snum,"poster_path":None,"episodes":[ep_doc]}],
        "added_at":      datetime.now(timezone.utc),
        "updated_at":    datetime.now(timezone.utc),
    }
    await col["series"].insert_one(doc)
    return slug, "inserted"

# ── Audit log ─────────────────────────────────────────────────
async def write_audit(action: str, collection: str, slug: str, detail: dict):
    await col["audit_logs"].insert_one({
        "actor":      "bot",
        "action":     action,
        "collection": collection,
        "slug":       slug,
        "detail":     detail,
        "timestamp":  datetime.now(timezone.utc),
    })

# ── Main handler ──────────────────────────────────────────────
@bot.on_message(filters.chat(DB_CHANNEL) & (filters.video | filters.document | filters.audio))
async def handle_db_message(client: Client, msg: Message):
    file_id  = get_file_id(msg)
    filename = get_filename(msg)
    caption  = (msg.caption or "").strip()
    size_mb  = get_size_mb(msg)
    msg_id   = msg.id

    if not file_id:
        return

    log.info(f"[MSG {msg_id}] {filename!r}")

    # 1. Parse structured caption first; fall back to AI
    info = parse_caption(caption)
    if not info:
        log.info(f"[MSG {msg_id}] No structured caption → using AI")
        info = await ai_classify(filename, caption)

    media_type = info.get("type","unknown")
    if media_type == "unknown" or not info.get("title"):
        log.warning(f"[MSG {msg_id}] Unclassifiable: {filename!r}")
        await _log_channel(msg, info, {}, "⚠️ UNCLASSIFIED", "")
        return

    slug, action = "", ""
    detail_doc   = {}

    async with httpx.AsyncClient() as http:
        tmdb_kind   = "movie" if media_type == "movie" else "tv"
        tmdb_result = await tmdb_search(http, info["title"], info.get("year"), tmdb_kind)
        tmdb_id     = tmdb_result["id"] if tmdb_result else None
        detail_doc  = await tmdb_detail(http, tmdb_id, tmdb_kind) if tmdb_id else {}

        if media_type == "movie":
            slug, action = await save_movie(info, detail_doc, file_id, msg_id, size_mb)
            collection   = "movies"
        else:
            ep_detail = {}
            if tmdb_id and info.get("season") and info.get("episode"):
                ep_detail = await tmdb_episode(http, tmdb_id, info["season"], info["episode"])
            slug, action = await save_episode(info, detail_doc, ep_detail, file_id, msg_id, size_mb)
            collection   = "series"

    # 2. Bot log (raw, for admin)
    await col["bot_logs"].insert_one({
        "msg_id":   msg_id, "filename": filename,
        "caption":  caption[:300], "ai_info": info,
        "slug":     slug, "action":   action,
        "tmdb_id":  detail_doc.get("id"),
        "timestamp":datetime.now(timezone.utc),
    })

    # 3. Audit log (structured, role-visible to superadmin)
    await write_audit(
        action     = action,
        collection = collection,
        slug       = slug,
        detail     = {"msg_id":msg_id, "quality":info.get("quality"), "filename":filename, "size_mb":size_mb}
    )

    # 4. Log channel message
    icon   = "🎬" if media_type == "movie" else "📺"
    status = "✅ NEW" if action == "inserted" else "🔄 UPDATED"
    await _log_channel(msg, info, detail_doc, f"{icon} {status}", slug)

async def _log_channel(msg, info, detail, status, slug):
    if not LOG_CHANNEL: return
    title = detail.get("title") or detail.get("name") or info.get("title","?")
    q     = info.get("quality","?")
    lang  = info.get("language","?").upper()
    text  = (
        f"{status}\n\n"
        f"**{title}**\n"
        f"`{info.get('release_status','')} · {q}p · {lang}`\n"
        f"Slug: `{slug}`\n"
        f"Confidence: `{info.get('confidence',0):.0%}`\n"
        f"Msg ID: `{msg.id}`"
    )
    try:
        await msg._client.send_message(LOG_CHANNEL, text)
    except Exception as e:
        log.warning(f"Log channel: {e}")

# ── Startup ───────────────────────────────────────────────────
async def ensure_indexes():
    await col["movies"].create_index("id", unique=True)
    await col["movies"].create_index([("title","text"),("overview","text")])
    await col["movies"].create_index("language")
    await col["movies"].create_index("genres")
    await col["movies"].create_index("added_at")
    await col["series"].create_index("id", unique=True)
    await col["series"].create_index([("title","text"),("overview","text")])
    await col["series"].create_index("language")
    await col["collections"].create_index("id", unique=True)
    await col["bot_logs"].create_index("timestamp")
    await col["audit_logs"].create_index("timestamp")
    await col["users"].create_index("username", unique=True)
    log.info("Indexes ready")

async def ensure_superadmin():
    """Create default superadmin if no users exist."""
    import hashlib
    count = await col["users"].count_documents({})
    if count == 0:
        pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
        await col["users"].insert_one({
            "username":   "superadmin",
            "password":   pw_hash,
            "role":       "superadmin",
            "created_at": datetime.now(timezone.utc),
            "active":     True,
        })
        log.info("Default superadmin created: superadmin / admin123  ← CHANGE THIS!")

async def main():
    await ensure_indexes()
    await ensure_superadmin()
    log.info(f"SC Files Bot v4 — watching {DB_CHANNEL}")
    await bot.start()
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
