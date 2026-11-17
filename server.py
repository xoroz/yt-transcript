# REF https://github.com/jdepoix/youtube-transcript-api
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from urllib.parse import urlparse, parse_qs
from html import escape
from pathlib import Path
import logging
import os
import re
import sqlite3
import threading
import time

from dotenv import load_dotenv
import requests
from starlette.middleware.base import BaseHTTPMiddleware

import youtube_transcript_api as yta
from youtube_transcript_api._errors import NoTranscriptFound
from youtube_transcript_api._settings import INNERTUBE_API_URL, INNERTUBE_CONTEXT
from youtube_transcript_api._transcripts import TranscriptList, TranscriptListFetcher
from youtube_transcript_api.proxies import WebshareProxyConfig


app = FastAPI()

# -----------------------
# Load .env
# -----------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)

DB_PATH = BASE_DIR / "cache.sqlite3"
MIN_SECONDS_BETWEEN_FETCHES = float(os.environ.get("YTA_MIN_FETCH_INTERVAL", "8"))

PROXY_USER = os.environ.get("YTA_PROXY_USER", "").strip()
PROXY_PASS = os.environ.get("YTA_PROXY_PASS", "").strip()
INNERTUBE_KEY_ENV = os.environ.get("YTA_INNERTUBE_API_KEY", "").strip()

DEFAULT_LANGUAGES = ["en", "en-US", "en-GB", "it", "pt", "es"]
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "www.youtu.be",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
}

# -----------------------
# Logging (/log folder)
# -----------------------
LOG_DIR = BASE_DIR / "log"
TRANSCRIPTS_DIR = LOG_DIR / "transcripts"
LOG_DIR.mkdir(parents=True, exist_ok=True)
TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

ops_logger = logging.getLogger("ops")
ops_logger.setLevel(logging.INFO)
if not ops_logger.handlers:
    _ops_handler = logging.FileHandler(LOG_DIR / "operations.log", encoding="utf-8")
    _ops_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    ops_logger.addHandler(_ops_handler)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "-"
        method = request.method
        path = request.url.path

        response = await call_next(request)

        ops_logger.info(f'ip={client_ip} method={method} path="{path}" status={response.status_code}')
        return response


app.add_middleware(LoggingMiddleware)

# -----------------------
# Cache + rate limiting
# -----------------------
_fetch_lock = threading.Lock()
_last_fetch_ts = 0.0


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS transcripts_v2 (
            video_id TEXT NOT NULL,
            language_code TEXT NOT NULL,
            text TEXT NOT NULL,
            is_generated INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (video_id, language_code)
        )"""
    )
    return conn


def extract_video_id(url: str) -> str:
    u = (url or "").strip()
    if not u:
        raise ValueError("URL is required")
    if VIDEO_ID_RE.fullmatch(u):
        return u
    if "://" not in u and any(u.lower().startswith(p) for p in ("youtube.com/", "www.youtube.com/", "m.youtube.com/", "music.youtube.com/", "youtu.be/", "www.youtu.be/")):
        u = "https://" + u

    p = urlparse(u)
    host = (p.hostname or "").lower()
    if p.scheme not in ("http", "https") or host not in YOUTUBE_HOSTS:
        raise ValueError("URL must be a YouTube link")

    if host in ("youtu.be", "www.youtu.be"):
        vid = (p.path or "").strip("/").split("/")[0]
        if VIDEO_ID_RE.fullmatch(vid):
            return vid

    q = parse_qs(p.query or "")
    if "v" in q and q["v"]:
        vid = q["v"][0].strip()
        if VIDEO_ID_RE.fullmatch(vid):
            return vid

    parts = [x for x in (p.path or "").split("/") if x]
    for i, part in enumerate(parts):
        if part in ("shorts", "embed", "live", "v") and i + 1 < len(parts):
            vid = parts[i + 1].strip()
            if VIDEO_ID_RE.fullmatch(vid):
                return vid

    raise ValueError("Could not extract a valid YouTube video id")


class Req(BaseModel):
    url: str | None = None
    video_id: str | None = None
    languages: list[str] | None = None
    no_cache: bool = False


class _TimeoutSession(requests.Session):
    def __init__(self, timeout=20):
        super().__init__()
        self._timeout = timeout

    def request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", self._timeout)
        return super().request(method, url, **kwargs)


def _innertube_key() -> str:
    if INNERTUBE_KEY_ENV:
        return INNERTUBE_KEY_ENV
    static_html = Path(yta.__file__).resolve().parent / "test" / "assets" / "youtube.html.static"
    try:
        m = re.search(r'"INNERTUBE_API_KEY":\s*"([a-zA-Z0-9_-]+)"', static_html.read_text())
        if m:
            return m.group(1)
    except OSError:
        pass
    return ""


def _safe_video_filename(video_id: str) -> str:
    safe_id = "".join(c for c in (video_id or "") if c.isalnum() or c in ("-", "_"))[:128]
    return safe_id or "unknown"


def _write_transcript_file(video_id: str, text: str):
    fname = _safe_video_filename(video_id) + ".txt"
    out_path = TRANSCRIPTS_DIR / fname
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text or "")


def _fetch_transcript_text(video_id: str, languages: list[str] | None) -> tuple[str, str, bool]:
    """Fetch transcript from YouTube (uncached) with rate limiting."""
    global _last_fetch_ts

    with _fetch_lock:
        now = time.time()
        wait = MIN_SECONDS_BETWEEN_FETCHES - (now - _last_fetch_ts)
        if wait > 0:
            time.sleep(wait)

        try:
            sess = _TimeoutSession(timeout=20)
            sess.headers.update({"Accept-Language": "en-US"})
            proxy = None
            if PROXY_USER and PROXY_PASS:
                proxy = WebshareProxyConfig(proxy_username=PROXY_USER, proxy_password=PROXY_PASS)
                sess.proxies = proxy.to_requests_dict()
                if proxy.prevent_keeping_connections_alive:
                    sess.headers.update({"Connection": "close"})

            tl = None
            key = _innertube_key()
            if key and proxy is None:
                resp = sess.post(
                    INNERTUBE_API_URL.format(api_key=key),
                    json={"context": INNERTUBE_CONTEXT, "videoId": video_id},
                )
                if resp.status_code == 200:
                    fetcher = TranscriptListFetcher(sess, None)
                    captions_json = fetcher._extract_captions_json(resp.json(), video_id)
                    tl = TranscriptList.build(sess, video_id, captions_json)

            if tl is None:
                fetcher = TranscriptListFetcher(sess, proxy)
                html = fetcher._fetch_video_html(video_id)
                fresh_key = fetcher._extract_innertube_api_key(html, video_id)
                data = fetcher._fetch_innertube_data(video_id, fresh_key)
                captions_json = fetcher._extract_captions_json(data, video_id)
                tl = TranscriptList.build(sess, video_id, captions_json)

            avail = list(tl)
            if not avail:
                raise ValueError("No transcripts available for this video")

            if languages:
                try:
                    t = tl.find_transcript(languages)
                except NoTranscriptFound:
                    codes = ", ".join(x.language_code for x in avail)
                    raise ValueError(f"Requested language(s) {languages} not available; found: {codes}")
            else:
                try:
                    t = tl.find_transcript(DEFAULT_LANGUAGES)
                except NoTranscriptFound:
                    manual = [x for x in avail if not x.is_generated]
                    t = manual[0] if manual else avail[0]

            snippets = t.fetch()
            text = " ".join(" ".join(s.text.split()) for s in snippets if s.text and s.text.strip())
            return text, t.language_code, bool(t.is_generated)
        finally:
            _last_fetch_ts = time.time()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/transcript")
def transcript(req: Req, request: Request):
    try:
        client_ip = request.client.host if request.client else "-"
        input_url = (req.url or "").strip()

        if req.video_id:
            video_id = extract_video_id(req.video_id)
        else:
            video_id = extract_video_id(input_url)

        langs = [x.strip() for x in (req.languages or []) if str(x).strip()] or None

        if not req.no_cache:
            conn = _db()
            try:
                rows = conn.execute(
                    "SELECT language_code, text, is_generated FROM transcripts_v2 WHERE video_id = ?",
                    (video_id,),
                ).fetchall()
            finally:
                conn.close()
            if rows:
                by_lang = {r[0]: (r[1], bool(r[2])) for r in rows}
                chosen_code = None
                for pref in (langs or DEFAULT_LANGUAGES):
                    if pref in by_lang:
                        chosen_code = pref
                        break
                if not chosen_code and not langs:
                    chosen_code = rows[0][0]
                if chosen_code:
                    text, is_gen = by_lang[chosen_code]
                    _write_transcript_file(video_id, text)
                    ops_logger.info(
                        f'ip={client_ip} op=transcript video_id={video_id} lang={chosen_code} video_url="{input_url}" cached=True text_len={len(text)}'
                    )
                    return {
                        "video_id": video_id,
                        "language_code": chosen_code,
                        "is_generated": is_gen,
                        "text": text,
                        "cached": True,
                    }

        text, lang_code, is_gen = _fetch_transcript_text(video_id, langs)

        conn = _db()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO transcripts_v2(video_id, language_code, text, is_generated, created_at) VALUES(?,?,?,?,?)",
                (video_id, lang_code, text, int(is_gen), int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()

        _write_transcript_file(video_id, text)
        ops_logger.info(
            f'ip={client_ip} op=transcript video_id={video_id} lang={lang_code} video_url="{input_url}" cached=False text_len={len(text)}'
        )

        return {
            "video_id": video_id,
            "language_code": lang_code,
            "is_generated": is_gen,
            "text": text,
            "cached": False,
        }

    except Exception as e:
        client_ip = request.client.host if request.client else "-"
        ops_logger.info(
            f'ip={client_ip} op=transcript error="{str(e)}" video_url="{(req.url or "").strip()}" video_id="{req.video_id or ""}"'
        )
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/transcript/text")
def transcript_text(req: Req, request: Request):
    result = transcript(req, request)
    text = " ".join((result.get("text") or "").split())
    return {
        "video_id": result.get("video_id"),
        "language_code": result.get("language_code"),
        "text": text,
        "cached": result.get("cached", False),
    }


@app.post("/transcript/html")
def transcript_html(req: Req, request: Request):
    result = transcript(req, request)
    safe = escape(result.get("text") or "")
    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<style>body{font-family:system-ui,Arial;line-height:1.5;max-width:900px;margin:2rem auto;padding:0 1rem}</style>"
        "</head><body>"
        f"<h1>Transcript</h1><p>{safe}</p>"
        "</body></html>"
    )
    return {"video_id": result.get("video_id"), "html": html, "cached": result.get("cached", False)}
