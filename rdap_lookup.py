import asyncio
import logging
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone

import asyncpg
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")
BATCH_SIZE = 500
WRITE_BATCH = 250
CONCURRENCY_PER_SERVER = 5
MAX_RETRIES = 10
BASE_BACKOFF = 1.0           # first retry after ~1s
MAX_BACKOFF = 120.0           # cap at 2 minutes
REQUEST_TIMEOUT = 45.0
IANA_BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"

# Worker auto-restart config
MAX_WORKER_CRASHES = 50       # auto-restart up to this many times
WORKER_CRASH_COOLDOWN = 30.0  # wait 30s before auto-restarting after crash

# ---------------------------------------------------------------------------
# RDAP bootstrap
# ---------------------------------------------------------------------------
_tld_to_server: dict[str, str] = {}
_server_semaphores: dict[str, asyncio.Semaphore] = {}


async def load_bootstrap(client: httpx.AsyncClient) -> None:
    for attempt in range(5):
        try:
            log.info("Fetching IANA RDAP bootstrap (attempt %d)", attempt + 1)
            resp = await client.get(IANA_BOOTSTRAP_URL, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for entry in data.get("services", []):
                tlds, urls = entry
                server_url = next((u for u in urls if u.startswith("https://")), urls[0])
                server_url = server_url.rstrip("/")
                for tld in tlds:
                    _tld_to_server[tld.lower()] = server_url
            log.info("Loaded RDAP servers for %d TLDs", len(_tld_to_server))
            return
        except Exception as exc:
            log.warning("Bootstrap fetch failed: %s (attempt %d/5)", exc, attempt + 1)
            await asyncio.sleep(5 * (attempt + 1))
    raise RuntimeError("Failed to load IANA RDAP bootstrap after 5 attempts")


def get_rdap_server(domain: str) -> str | None:
    parts = domain.rsplit(".", 1)
    if len(parts) < 2:
        return None
    return _tld_to_server.get(parts[1].lower())


def _get_semaphore(server_url: str) -> asyncio.Semaphore:
    if server_url not in _server_semaphores:
        _server_semaphores[server_url] = asyncio.Semaphore(CONCURRENCY_PER_SERVER)
    return _server_semaphores[server_url]


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped at MAX_BACKOFF."""
    delay = min(BASE_BACKOFF * (2 ** attempt), MAX_BACKOFF)
    jitter = random.uniform(0, delay * 0.3)
    return delay + jitter


# ---------------------------------------------------------------------------
# RDAP lookup
# ---------------------------------------------------------------------------
def _parse_registration_date(data: dict) -> datetime | None:
    events = data.get("events", [])
    for ev in events:
        action = ev.get("eventAction", "").lower()
        if action == "registration":
            try:
                return datetime.fromisoformat(ev["eventDate"].replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue
    fallback_actions = {"last changed of registration", "registrationdate"}
    for ev in events:
        action = ev.get("eventAction", "").lower()
        if action in fallback_actions:
            try:
                return datetime.fromisoformat(ev["eventDate"].replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue
    return None


async def fetch_registration_date(
    client: httpx.AsyncClient, domain: str, server_url: str,
) -> tuple[str, datetime | None]:
    """Query RDAP for a single domain with aggressive retry logic."""
    sem = _get_semaphore(server_url)
    url = f"{server_url}/domain/{domain}"

    for attempt in range(MAX_RETRIES):
        try:
            async with sem:
                resp = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)

            if resp.status_code == 404:
                return (domain, None)

            # 4xx client errors (except 429) = permanent, skip immediately
            if resp.status_code in (400, 403, 410):
                return (domain, None)

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", _backoff(attempt)))
                retry_after = min(retry_after, MAX_BACKOFF)
                if attempt < 3:
                    log.debug("429 for %s — backing off %.1fs", domain, retry_after)
                else:
                    log.warning("429 for %s — backing off %.1fs (attempt %d/%d)", domain, retry_after, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                delay = _backoff(attempt)
                log.warning("HTTP %d for %s — retrying in %.1fs (attempt %d/%d)", resp.status_code, domain, delay, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            return (domain, _parse_registration_date(resp.json()))

        except Exception as exc:
            delay = _backoff(attempt)
            if attempt >= 2:
                log.warning("Error for %s: %s — retrying in %.1fs (attempt %d/%d)", domain, type(exc).__name__, delay, attempt + 1, MAX_RETRIES)
            await asyncio.sleep(delay)

    log.error("Exhausted %d retries for %s — skipping", MAX_RETRIES, domain)
    return (domain, None)


async def process_batch(client: httpx.AsyncClient, domains: list[str]) -> list[tuple[str, datetime]]:
    tasks = []
    skipped = 0
    for domain in domains:
        server = get_rdap_server(domain)
        if server is None:
            skipped += 1
            continue
        tasks.append(fetch_registration_date(client, domain, server))
    if skipped:
        log.info("Skipped %d domains with no known RDAP server", skipped)
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Filter out any exceptions that somehow slipped through
    good = []
    for r in results:
        if isinstance(r, Exception):
            log.warning("Unexpected gather exception: %s", r)
            continue
        domain, dt = r
        if dt is not None:
            good.append((domain, dt))
    return good


async def update_db(pool: asyncpg.Pool, results: list[tuple[str, datetime]]) -> int:
    """Batch-update with retry on DB connection errors."""
    if not results:
        return 0
    for attempt in range(5):
        try:
            async with pool.acquire() as conn:
                await conn.executemany(
                    "UPDATE global_domains SET registered_at = $1 WHERE name = $2",
                    [(dt, domain) for domain, dt in results],
                )
            return len(results)
        except Exception as exc:
            delay = _backoff(attempt)
            log.warning("DB write failed: %s — retrying in %.1fs (attempt %d/5)", exc, delay, attempt + 1)
            await asyncio.sleep(delay)
    log.error("DB write failed after 5 attempts — dropping %d results", len(results))
    return 0


# ---------------------------------------------------------------------------
# Worker — self-healing background task
# ---------------------------------------------------------------------------
class RDAPWorker:
    def __init__(self) -> None:
        self.running = False
        self.task: asyncio.Task | None = None
        self.round_num = 0
        self.total_updated = 0
        self.started_at: float | None = None
        self.logs: deque[str] = deque(maxlen=500)
        self.rate: float = 0.0
        self.crash_count = 0
        self._pool: asyncpg.Pool | None = None

    def _log(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.logs.append(entry)
        log.info(msg)

    async def _get_pool(self) -> asyncpg.Pool:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        if self._pool is not None:
            # Test the pool is alive
            try:
                async with self._pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            except Exception:
                log.warning("DB pool unhealthy — recreating")
                try:
                    await self._pool.close()
                except Exception:
                    pass
                self._pool = None
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=5,
                command_timeout=60, server_settings={"statement_timeout": "60000"},
            )
        return self._pool

    async def get_progress(self) -> dict:
        if not DATABASE_URL:
            return {
                "running": False, "round": 0, "total": 0, "done": 0,
                "remaining": 0, "pct": 0, "total_updated_this_session": 0,
                "rate": 0, "elapsed_min": 0, "crash_count": 0,
                "error": "DATABASE_URL not set",
            }
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT count(*) AS total,
                           count(registered_at) AS done,
                           count(*) - count(registered_at) AS remaining
                    FROM global_domains
                    """
                )
        except Exception as e:
            return {
                "running": self.running, "round": self.round_num, "total": 0, "done": 0,
                "remaining": 0, "pct": 0, "total_updated_this_session": self.total_updated,
                "rate": self.rate, "elapsed_min": 0, "crash_count": self.crash_count,
                "error": str(e),
            }
        total, done, remaining = row["total"], row["done"], row["remaining"]
        pct = round(done / total * 100, 2) if total else 0
        elapsed = time.time() - self.started_at if self.started_at and self.running else 0
        return {
            "running": self.running,
            "round": self.round_num,
            "total": total,
            "done": done,
            "remaining": remaining,
            "pct": pct,
            "total_updated_this_session": self.total_updated,
            "rate": round(self.rate, 1),
            "elapsed_min": round(elapsed / 60, 1),
            "crash_count": self.crash_count,
        }

    async def _run(self) -> None:
        self._log("Worker starting")
        self.started_at = time.time()
        pool = await self._get_pool()

        async with httpx.AsyncClient(
            headers={"Accept": "application/rdap+json, application/json"},
            http2=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=15.0),
        ) as client:
            if not _tld_to_server:
                await load_bootstrap(client)

            while self.running:
                self.round_num += 1
                round_start = time.time()

                # Fetch next batch — with retry on DB failure
                rows = None
                for db_attempt in range(5):
                    try:
                        pool = await self._get_pool()
                        async with pool.acquire() as conn:
                            rows = await conn.fetch(
                                "SELECT name FROM global_domains WHERE registered_at IS NULL LIMIT $1",
                                BATCH_SIZE,
                            )
                        break
                    except Exception as exc:
                        delay = _backoff(db_attempt)
                        self._log(f"DB fetch failed: {exc} — retrying in {delay:.0f}s")
                        await asyncio.sleep(delay)

                if rows is None:
                    self._log("Could not fetch domains after 5 DB retries — pausing 60s")
                    await asyncio.sleep(60)
                    continue

                if not rows:
                    self._log("No more domains to process — all done!")
                    self.running = False
                    break

                domains = [r["name"] for r in rows]
                self._log(f"Round {self.round_num}: processing {len(domains)} domains")

                all_results: list[tuple[str, datetime]] = []
                for i in range(0, len(domains), WRITE_BATCH):
                    if not self.running:
                        break
                    chunk = domains[i : i + WRITE_BATCH]
                    results = await process_batch(client, chunk)
                    all_results.extend(results)

                written = await update_db(pool, all_results)
                self.total_updated += written

                elapsed = time.time() - round_start
                self.rate = len(domains) / elapsed if elapsed > 0 else 0

                self._log(
                    f"Round {self.round_num} done: wrote {written}/{len(domains)} | "
                    f"session total: {self.total_updated} | "
                    f"rate: {self.rate:.1f} domains/s"
                )

        self._log("Worker stopped")

    def start(self) -> bool:
        if self.running:
            return False
        self.running = True
        self.round_num = 0
        self.total_updated = 0
        self.crash_count = 0
        self.task = asyncio.create_task(self._safe_run())
        return True

    async def _safe_run(self) -> None:
        """Run the worker with auto-restart on crash."""
        while self.running and self.crash_count < MAX_WORKER_CRASHES:
            try:
                await self._run()
                break  # Clean exit (finished all domains or stopped)
            except Exception as e:
                self.crash_count += 1
                self._log(
                    f"Worker crashed ({self.crash_count}/{MAX_WORKER_CRASHES}): "
                    f"{type(e).__name__}: {e}"
                )
                log.exception("Worker crashed")

                if self.crash_count >= MAX_WORKER_CRASHES:
                    self._log("Max crash limit reached — giving up")
                    break

                self._log(f"Auto-restarting in {WORKER_CRASH_COOLDOWN:.0f}s...")
                await asyncio.sleep(WORKER_CRASH_COOLDOWN)
                self._log("Restarting worker...")

        self.running = False

    def stop(self) -> bool:
        if not self.running:
            return False
        self.running = False
        self._log("Stop requested — finishing current batch")
        return True


worker = RDAPWorker()
