# YouTube Downloader

A single-file helper (`download_vod.py`) that pulls a **YouTube video, thumbnails, chat replay and metadata** for one channel and writes them into your Archiver folder.

```
persons/<STREAMER>/youtube/VODs/<ISO-date>*ytv-<ID>*<sanitised-title>/
├─ vod.mp4
├─ chat.yt-vod.sqlite
├─ thumbnails/
│  ├─ thumbnail\_main.jpg
│  └─ thumbnail\_0001.jpg …
└─ metadata.yt-vod.json
````

*Save-path format:* `[date]_[id]_[title]` (`:` replaced by `-`, title truncated to 100 chars).

---

## Requirements

* Python 3.11+
* `yt-dlp` 2025-xx (bundled in `.dependencies/yt-dlp` or on `$PATH`)
* `ffmpeg` / `ffprobe` (bundled in `.dependencies/ffmpeg` or on `$PATH`)

---

## Quick usage

```bash
# activate your venv first
python download_vod.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
````

* Adds SHA-256, duration, view/like counts and more to `metadata.yt-vod.json`.
* Converts chat JSON to `chat.yt-vod.sqlite` (table `chat_messages`).
* Deduplicates thumbnails and keeps the largest one as `thumbnail_main.jpg`.
* Logs to stdout; use `--debug` for verbose output.
