import logging
import logging.handlers
import time
from pathlib import Path
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.routing import Mount, Route

DIST_DIR = Path("/opt/canui/dist")
INDEX_HTML = DIST_DIR / "index.html"

# ── Logging setup ──────────────────────────────────────────────────────────
LOG_FILE = "/home/geekoma6database/Documents/logs/CANdatabase"

handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)-8s] [UI ] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

log = logging.getLogger("candb.ui")
log.setLevel(logging.DEBUG)
log.addHandler(handler)

for lib in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    lib_logger = logging.getLogger(lib)
    lib_logger.setLevel(logging.DEBUG)
    lib_logger.addHandler(handler)
# ──────────────────────────────────────────────────────────────────────────


class AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        t0 = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            log.error("UNHANDLED exception serving %s: %s", request.url.path, exc, exc_info=True)
            raise
        elapsed = (time.perf_counter() - t0) * 1000
        level = logging.WARNING if response.status_code >= 400 else logging.DEBUG
        log.log(level, "%s %s  status=%s  %.1fms  client=%s",
                request.method, request.url.path, response.status_code, elapsed, request.client)
        return response


_NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}

async def serve_index(request: Request) -> FileResponse:
    """Serve index.html with no-cache headers so browsers always fetch the latest bundle."""
    return FileResponse(INDEX_HTML, headers=_NO_CACHE_HEADERS)


log.info("=== candb-ui starting up ===")

app = Starlette(routes=[
    Route("/", serve_index),
    Route("/index.html", serve_index),
    Mount("/", app=StaticFiles(directory=str(DIST_DIR))),
])
app.add_middleware(AccessLogMiddleware)
