from __future__ import annotations

import argparse, datetime as dt, hashlib, json, logging, os, random, re, shlex
import sqlite3, string, subprocess
from pathlib import Path
from typing import Any, Dict, List

CF = 10
HERE = Path(__file__).resolve()
ARCHIVER_ROOT = HERE.parents[3]
DEPS_DIR = HERE.parents[2] / ".dependencies"
YTDLP_BIN = (DEPS_DIR / "yt-dlp" / "yt-dlp.exe").resolve()
FFMPEG_DIR = (DEPS_DIR / "ffmpeg").resolve()
os.environ["PATH"] = f"{FFMPEG_DIR}{os.pathsep}{os.getenv('PATH','')}"
if not YTDLP_BIN.exists():
    YTDLP_BIN = "yt-dlp"

STREAMER, PLATFORM, SECTION = "MichiMochievee", "youtube", "VODs"
BASE_OUTDIR = ARCHIVER_ROOT / "persons" / STREAMER / PLATFORM / SECTION
LOCAL_ID_PREFIX, RANDOM_ID_DIGITS = "yt-2localvt22", 6
_illegal_fs_chars = re.compile(r"[<>:\"/\\|?*\0-\37]")

CHAT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS chat_messages (
  message_id            TEXT PRIMARY KEY,
  message_sent_absolute TEXT,
  message_sent_offset   INTEGER,
  user_name             TEXT,
  user_id               TEXT,
  user_logo             TEXT,
  message_body          TEXT,
  donation              TEXT,
  color                 TEXT,
  message_type          TEXT,
  is_pinned             INTEGER,
  author_badges         TEXT
)
"""


def sanitise(s: str, max_len: int = 100) -> str:
    s = _illegal_fs_chars.sub("", s)[:max_len].strip()
    s = s.rstrip(". ")
    return s or "untitled"


def sha256f(p: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for blk in iter(lambda: f.read(chunk), b""):
            h.update(blk)
    return h.hexdigest()


def local_id() -> str:
    return (
        f"{LOCAL_ID_PREFIX}{''.join(random.choices(string.digits,k=RANDOM_ID_DIGITS))}"
    )


def iso_from_ts(ts: int | str | None) -> str | None:
    if not ts:
        return None
    if isinstance(ts, str):
        ts = int(ts)
    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).isoformat().replace("+00:00", "Z")


def iso_from_usec(us: int | str | None) -> str | None:
    if not us:
        return None
    if isinstance(us, str):
        us = int(us)
    return (
        dt.datetime.fromtimestamp(us / 1_000_000, tz=dt.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def run_ydlp(args: List[str], capture: bool, log: logging.Logger) -> str:
    cmd = [str(YTDLP_BIN), *args]
    log.debug("Exec: %s", shlex.join(cmd))
    if capture:
        res = subprocess.run(cmd, text=True, capture_output=True)
        if res.returncode:
            raise RuntimeError(f"yt-dlp exited {res.returncode}")
        return res.stdout
    proc = subprocess.Popen(cmd)
    proc.communicate()
    if proc.returncode:
        raise RuntimeError(f"yt-dlp exited {proc.returncode}")
    return ""


RENDERER_KEYS = (
    "liveChatTextMessageRenderer",
    "liveChatPaidMessageRenderer",
    "liveChatPaidStickerRenderer",
    "liveChatStickerRenderer",
    "liveChatViewerEngagementMessageRenderer",
)


def iter_top_objs(path: Path):
    text = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(text.lstrip())
        yield from (parsed if isinstance(parsed, list) else [parsed])
        return
    except json.JSONDecodeError:
        pass
    for line in text.splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def extract_renderer(action):
    """Return (renderer_obj, renderer_type_key, pinned_flag) or (None,'',False)."""
    if "addChatItemAction" in action:
        item = action["addChatItemAction"]["item"]
        key = next((k for k in item if k in RENDERER_KEYS), None)
        if key:
            return item[key], key, False
        return None, "", False

    if "addBannerToLiveChatCommand" in action:
        banner = action["addBannerToLiveChatCommand"]["bannerRenderer"][
            "liveChatBannerRenderer"
        ]
        contents = banner["contents"]
        key = next((k for k in contents if k in RENDERER_KEYS), None)
        if key:
            return contents[key], key, True
        return None, "", False

    return None, "", False


def badge_list(badges):
    return ";".join(
        b.get("liveChatAuthorBadgeRenderer", {})
        .get("icon", {})
        .get("iconType", "")
        .upper()
        for b in badges
        if "liveChatAuthorBadgeRenderer" in b
    )


def render_runs(runs):
    out = []
    for r in runs:
        if "text" in r:
            out.append(r["text"])
        elif "emoji" in r:
            e = r["emoji"]
            out.append(
                e["shortcuts"][0] if e.get("shortcuts") else e.get("emojiId", "")
            )
    return "".join(out)


def chat_json_to_sqlite(raw: Path, sqlite_path: Path, log):
    rows = []
    for top in iter_top_objs(raw):
        if "replayChatItemAction" not in top:
            continue
        offset_ms = int(top["replayChatItemAction"].get("videoOffsetTimeMsec", "0"))
        for act in top["replayChatItemAction"]["actions"]:
            ren, rkey, pinned = extract_renderer(act)
            if ren is None:
                continue

            mid = ren.get("id")
            sent = iso_from_usec(ren.get("timestampUsec"))
            author = ren.get("authorName", {}).get("simpleText")
            aid = ren.get("authorExternalChannelId")
            logo = (ren.get("authorPhoto", {}).get("thumbnails", [{}])[0]).get("url")
            color = ren.get("bodyBackgroundColor") or ren.get("headerBackgroundColor")

            if "message" in ren:
                body = render_runs(ren["message"].get("runs", []))
            elif "accessibility" in ren:
                body = ren["accessibility"]["accessibilityData"]["label"]
            else:
                body = None

            donation = None
            if "purchaseAmountMicros" in ren:
                amt = int(ren["purchaseAmountMicros"]) / 1_000_000
                cur = ren.get("currency") or ren.get("purchaseCurrency", "")
                donation = f"{amt:.2f}; {cur}; {color}"

            badges = badge_list(ren.get("authorBadges", []))
            mtype = (
                "paid"
                if "Paid" in rkey
                else (
                    "sticker"
                    if "Sticker" in rkey
                    else "system" if "ViewerEngagement" in rkey else "text"
                )
            )

            rows.append(
                (
                    mid,
                    sent,
                    offset_ms // 1000,
                    author,
                    aid,
                    logo,
                    body,
                    donation,
                    color,
                    mtype,
                    int(pinned),
                    badges,
                )
            )

    log.info("Inserting %d chat messages", len(rows))
    con = sqlite3.connect(sqlite_path)
    con.execute(CHAT_SCHEMA_SQL)
    if rows:
        con.executemany(
            "INSERT OR IGNORE INTO chat_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )
    con.commit()
    con.close()


def iso_end(meta):
    s, d = meta.get("release_timestamp") or meta.get("timestamp"), meta.get("duration")
    return iso_from_ts(int(s) + int(d)) if s and d else None


def write_meta_json(meta: Dict[str, Any], out: Path, vod_sha: str, log: logging.Logger):
    rec = {
        "stream_id": None,
        "vod_id": f"v{meta['id']}",
        "title": meta.get("title"),
        "created_at": iso_from_ts(
            meta.get("release_timestamp") or meta.get("timestamp")
        ),
        "published_at": iso_from_ts(meta.get("upload_date")),
        "thumbnail_url": meta.get("thumbnail"),
        "thumbnail_filename": "thumbnail_main.jpg",
        "url": meta.get("webpage_url"),
        "downloaded_at": dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "vod_sha256": vod_sha,
        "duration": meta.get("duration"),
        "duration_string": meta.get("duration_string")
        or str(dt.timedelta(seconds=meta.get("duration") or 0)),
        "start_time": iso_from_ts(
            meta.get("release_timestamp") or meta.get("timestamp")
        ),
        "end_time": iso_end(meta),
        "initial_title": meta.get("title"),
        "language": meta.get("language"),
        "origin": "youtube",
        "like_count": meta.get("like_count"),
        "comment_count": meta.get("comment_count"),
        "view_count": meta.get("view_count"),
        "availability": meta.get("availability"),
        "resolution": meta.get("resolution"),
        "fps": meta.get("fps"),
        "channel": meta.get("channel"),
        "channel_follower_count": meta.get("channel_follower_count"),
        "channel_is_verified": meta.get("channel_is_verified"),
        "description": meta.get("description"),
        "tags": meta.get("tags"),
    }

    out.write_text(json.dumps(rec, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Metadata → %s", out.name)

    if rec["description"]:
        desc_txt = out.with_name("description.yt-vod.txt")
        desc_txt.write_text(rec["description"], encoding="utf-8")
        log.info("Description → %s", desc_txt.name)


def main(url: str, debug: bool):
    logging.basicConfig(
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG if debug else logging.INFO,
        force=True,
    )
    log = logging.getLogger("yt-archiver")
    info = json.loads(
        run_ydlp(
            ["--dump-single-json", "--skip-download", "--no-warnings", url], True, log
        )
    )
    vid = info.get("id") or local_id()
    start_iso = iso_from_ts(info.get("release_timestamp") or info.get("timestamp"))
    start_dt = (
        dt.datetime.fromisoformat(start_iso.rstrip("Z"))
        if start_iso
        else dt.datetime.now(dt.UTC)
    )
    outdir = (
        BASE_OUTDIR
        / f"{start_dt.strftime('%Y-%m-%dT%H-%M-%SZ')}_yt-v{vid}_{sanitise(info.get('title',''))}"
    )
    outdir.mkdir(parents=True, exist_ok=True)
    log.info("Output dir: %s", outdir)

    vod_mp4 = outdir / "vod.mp4"
    if not vod_mp4.exists():
        run_ydlp(
            [
                "--ignore-errors",
                "--concurrent-fragments",
                str(CF),
                "--write-thumbnail",
                "--convert-thumbnails",
                "jpg",
                "--write-info-json",
                "--write-subs",
                "--sub-langs",
                "live_chat",
                "--sub-format",
                "json",
                "--merge-output-format",
                "mp4",
                "-o",
                "vod.%(ext)s",
                "-f",
                "bestvideo+bestaudio/best",
                "--paths",
                f"home:{outdir}",
                url,
            ],
            False,
            log,
        )
        for p in outdir.glob("*.live_chat.json"):
            p.rename(outdir / "chat_raw.json")
        info_file = next(outdir.glob("*.info.json"), None)
        if info_file:
            info_file.rename(outdir / "metadata_raw.json")

    thumb_dir = outdir / "thumbnails"
    thumb_dir.mkdir(exist_ok=True)
    thumbs = list(outdir.glob("*.jpg"))
    if thumbs:
        seen = set()
        uniq = []
        for p in thumbs:
            h = sha256f(p)
            if h in seen:
                p.unlink()
            else:
                seen.add(h)
                uniq.append(p)
        uniq.sort(key=lambda p: p.stat().st_size, reverse=True)
        for i, p in enumerate(uniq):
            p.rename(
                thumb_dir
                / ("thumbnail_main.jpg" if i == 0 else f"thumbnail_{i:04d}.jpg")
            )

    if (outdir / "chat_raw.json").exists() and not (
        outdir / "chat.yt-vod.sqlite"
    ).exists():
        chat_json_to_sqlite(
            outdir / "chat_raw.json", outdir / "chat.yt-vod.sqlite", log
        )
        (outdir / "chat_raw.json").unlink()

    if (outdir / "metadata_raw.json").exists() and not (
        outdir / "metadata.yt-vod.json"
    ).exists():
        write_meta_json(
            json.loads((outdir / "metadata_raw.json").read_text(encoding="utf-8")),
            outdir / "metadata.yt-vod.json",
            sha256f(vod_mp4),
            log,
        )
        (outdir / "metadata_raw.json").unlink()
    log.info("✔ Done.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--debug", action="store_true")
    main(**vars(ap.parse_args()))
