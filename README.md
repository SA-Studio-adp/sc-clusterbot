# SC Files Bot v4

Telegram bot that auto-classifies files from your DB channel using Claude AI → MongoDB.

## Caption format (structured — bot reads this first)
```
Title: Kaithi
Year: 2019
Quality: 1080p
Language: Tamil
Type: Movie
Season: -
Episode: -
Extras: PreDVD
```
If no structured caption, Claude AI classifies by filename.

## Setup
1. `pip install -r requirements.txt`
2. `cp .env.example .env` and fill in all values
3. Add bot as **admin** in your DB channel
4. `python main.py`
5. Backfill existing history: `python backfill.py`

## Deploy to Railway
Push to GitHub → New Railway project → add env vars → done.

## Default superadmin
Username: `superadmin` / Password: `admin123` — **change immediately via Admin Panel → Settings → Change Password**
