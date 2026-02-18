## #!/home/felix/projects/yt-transcript/venv/bin/python3.12
# REF https://github.com/jdepoix/youtube-transcript-api
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from urllib.parse import urlparse, parse_qs
from html import escape
from pathlib import Path
import sqlite3
import time
import threading
import os
import logging

from dotenv import load_dotenv
from starlette.middleware.base import BaseHTTPMiddleware

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig


app = FastAPI()

# -----------------------
# Load .env
# -----------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)

DB_PATH = BASE_DIR / "cache.sqlite3"
MIN_SECONDS_BETWEEN_FETCHES = float(os.environ.get("YTA_MIN_FETCH_INTERVAL", "8"))

# Proxy creds from .env (domain/port accepted but not required by WebshareProxyConfig)
PROXY_DOMAIN = os.environ.get("YTA_PROXY_DOMAIN", "").strip()
PROXY_PORT = os.environ.get("YTA_PROXY_PORT", "").strip()
PROXY_USER = os.environ.get("YTA_PROXY_USER", "").strip()
PROXY_PASS = os.environ.get("YTA_PROXY_PASS", "").strip()

# -----------------------
# Logging (/log folder)
# -----------------------
LOG_DIR = BASE_DIR / "log"
TRANSCRIPTS_DIR = LOG_DIR / "transcripts"
LOG_DIR.mkdir(parents=True, exist_ok=True)
TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

ops_logger = logging.getLogger("ops")
ops_logger.setLevel(logging.INFO)

_ops_handler = logging.FileHandler(LOG_DIR / "operations.log", encoding="utf-8")
_ops_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
ops_logger.addHandler(_ops_handler)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "-"
        method = request.method
        path = request.url.path

        response = await call_next(request)

        # Logs status code + request path + IP (middleware pattern) [2]
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
        """CREATE TABLE IF NOT EXISTS transcripts (
            video_id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )"""
    )
    return conn


def extract_video_id(url: str) -> str:
    u = (url or "").strip()
    p = urlparse(u)

    if "youtu.be" in (p.netloc or ""):
        vid = (p.path or "").strip("/").split("/")[0]
        if vid:
            return vid

    q = parse_qs(p.query or "")
    if "v" in q and q["v"]:
        return q["v"][0]

    parts = [x for x in (p.path or "").split("/") if x]
    for i, part in enumerate(parts):
        if part in ("shorts", "embed") and i + 1 < len(parts):
            return parts[i + 1]

    raise ValueError("Could not extract video id")


class Req(BaseModel):
    url: str | None = None
    video_id: str | None = None
    languages: list[str] = ["en", "en-US", "it"]
    no_cache: bool = False


def _make_api() -> YouTubeTranscriptApi:
    """
    youtube-transcript-api==1.2.3:
    Use WebshareProxyConfig(proxy_username, proxy_password) as per guide [1].
    """
    if PROXY_USER and PROXY_PASS:
        return YouTubeTranscriptApi(
            proxy_config=WebshareProxyConfig(
                proxy_username=PROXY_USER,
                proxy_password=PROXY_PASS,
            )
        )
    return YouTubeTranscriptApi()


def _safe_video_filename(video_id: str) -> str:
    safe_id = "".join(c for c in (video_id or "") if c.isalnum() or c in ("-", "_"))[:128]
    return safe_id or "unknown"


def _write_transcript_file(video_id: str, text: str):
    # One file per video id: /log/transcripts/<video_id>.txt
    fname = _safe_video_filename(video_id) + ".txt"
    out_path = TRANSCRIPTS_DIR / fname
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text or "")


def _fetch_transcript_text(video_id: str, languages: list[str]) -> str:
    """
    Fetches transcript from YouTube (uncached).
    Enforces a global lock + delay to avoid bursts that trigger blocking.
    """
    global _last_fetch_ts

    with _fetch_lock:
        now = time.time()
        wait = MIN_SECONDS_BETWEEN_FETCHES - (now - _last_fetch_ts)
        if wait > 0:
            time.sleep(wait)

        ytt_api = _make_api()

        # Stable flow: list -> find_transcript -> fetch
        tl = ytt_api.list(video_id)
        t = tl.find_transcript(languages)
        snippets = t.fetch()

        text = " ".join(s.text for s in snippets)

        _last_fetch_ts = time.time()
        return text


@app.post("/transcript")
def transcript(req: Req, request: Request):
    try:
        client_ip = request.client.host if request.client else "-"
        input_url = (req.url or "").strip()

        video_id = req.video_id or extract_video_id(input_url)

        # Cache lookup
        if not req.no_cache:
            conn = _db()
            row = conn.execute("SELECT text FROM transcripts WHERE video_id = ?", (video_id,)).fetchone()
            conn.close()
            if row:
                text = row[0]
                _write_transcript_file(video_id, text)

                # Operation detail log (includes video URL/ID and cached flag)
                ops_logger.info(
                    f'ip={client_ip} op=transcript video_id={video_id} video_url="{input_url}" cached=True text_len={len(text)}'
                )

                return {"video_id": video_id, "text": text, "cached": True}

        # Fetch from YouTube (rate-limited)
        text = _fetch_transcript_text(video_id, req.languages)

        # Store in cache
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO transcripts(video_id, text, created_at) VALUES(?,?,?)",
            (video_id, text, int(time.time())),
        )
        conn.commit()
        conn.close()

        _write_transcript_file(video_id, text)

        # Operation detail log
        ops_logger.info(
            f'ip={client_ip} op=transcript video_id={video_id} video_url="{input_url}" cached=False text_len={len(text)}'
        )

        return {"video_id": video_id, "text": text, "cached": False}

    except Exception as e:
        # Log the failure details (HTTP 400 returned below)
        client_ip = request.client.host if request.client else "-"
        ops_logger.info(
            f'ip={client_ip} op=transcript error="{str(e)}" video_url="{(req.url or "").strip()}" video_id="{req.video_id or ""}"'
        )
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/transcript/text")
def transcript_text(req: Req, request: Request):
    # Ensure single-line-ish text (spaces)
    result = transcript(req, request)
    text = " ".join((result.get("text") or "").split())
    return {"video_id": result.get("video_id"), "text": text, "cached": result.get("cached", False)}


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
