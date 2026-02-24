import asyncio
import logging
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urlparse

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
MAX_RETRIES = 10
MAX_429_RETRIES = 3           # give up on a domain after 3 rate-limit hits
BASE_BACKOFF = 1.0           # first retry after ~1s
MAX_BACKOFF = 120.0           # cap at 2 minutes
REQUEST_TIMEOUT = 45.0
IANA_BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"

# Worker auto-restart config
MAX_WORKER_CRASHES = 50       # auto-restart up to this many times
WORKER_CRASH_COOLDOWN = 30.0  # wait 30s before auto-restarting after crash

# ---------------------------------------------------------------------------
# Per-server rate limits (requests per second)
# Based on published docs, AUPs, and empirical testing.
# ---------------------------------------------------------------------------
DEFAULT_SERVER_RATE = 1.0     # conservative default for unknown servers

_KNOWN_SERVER_RATES: dict[str, float] = {
    # Verisign — .com, .net, .cc, .name — undisclosed limit, large capacity
    "rdap.verisign.com": 2.0,
    # Identity Digital / Donuts — 200+ gTLDs — documented ~10 rps for WHOIS
    "rdap.identitydigital.services": 5.0,
    "rdap.donuts.co": 5.0,
    # Afilias — .info, .org — moderate capacity
    "rdap.afilias.net": 2.0,
    "rdap.org.nic.info": 2.0,
    # CentralNic — 100+ gTLDs — documented 1,800/15min = 2/s
    "rdap.centralnic.com": 1.5,
    # Nominet — .uk — documented 5/s but 1K/day cap
    "rdap.nominet.uk": 3.0,
    # GMO Registry — .shop etc — undisclosed
    "rdap.gmoregistry.net": 1.0,
    # Australian ccTLD — .au, .com.au — very strict, 429s immediately
    "rdap.cctld.au": 0.5,
    "rdap.auda.org.au": 0.5,
    # DENIC — .de — undisclosed, GDPR-strict, pilot RDAP
    "rdap.denic.de": 1.0,
    # AFNIC — .fr — undisclosed
    "rdap.nic.fr": 1.0,
    # PIR — .org
    "rdap.publicinterestregistry.org": 2.0,
    # Neustar/GoDaddy Registry — various gTLDs
    "rdap.nic.godaddy": 2.0,
    # Google Registry — .google, .app, .dev, .page etc
    "rdap.nic.google": 2.0,
    # Amazon Registry
    "rdap.nic.amazon": 1.0,
}

# ---------------------------------------------------------------------------
# RDAP bootstrap
# ---------------------------------------------------------------------------
_tld_to_server: dict[str, str] = {}


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


# TLDs to skip entirely (broken RDAP servers or irrelevant)
SKIP_TLDS = {"gov", "mil", "edu", "int"}


def get_rdap_server(domain: str) -> str | None:
    parts = domain.rsplit(".", 1)
    if len(parts) < 2:
        return None
    tld = parts[1].lower()
    if tld in SKIP_TLDS:
        return None
    return _tld_to_server.get(tld)


# ---------------------------------------------------------------------------
# Per-server rate limiter
# ---------------------------------------------------------------------------
class _RateLimiter:
    """Controls both concurrency and sending rate for a single RDAP server.

    Uses a semaphore for max in-flight requests and a lock-guarded timestamp
    to enforce minimum spacing between request starts.
    """

    def __init__(self, rate: float):
        self.rate = rate
        # Scale max concurrency with rate: faster servers get more slots
        self.max_concurrent = max(2, min(int(rate * 3), 8))
        self._sem = asyncio.Semaphore(self.max_concurrent)
        self._interval = 1.0 / rate
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._sem.acquire()
        # Enforce minimum spacing between requests
        async with self._lock:
            now = time.time()
            wait = self._interval - (now - self._last_request_time)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_time = time.time()
        return self

    async def __aexit__(self, *args):
        self._sem.release()

    def throttle(self) -> None:
        """Halve the rate after a 429 — minimum 0.1 req/s."""
        old = self.rate
        self.rate = max(self.rate * 0.5, 0.1)
        self._interval = 1.0 / self.rate
        log.info("Rate for server throttled: %.2f → %.2f req/s", old, self.rate)


_rate_limiters: dict[str, _RateLimiter] = {}


def _get_rate_limiter(server_url: str) -> _RateLimiter:
    if server_url not in _rate_limiters:
        host = urlparse(server_url).hostname or ""
        rate = DEFAULT_SERVER_RATE
        for known_host, known_rate in _KNOWN_SERVER_RATES.items():
            if known_host in host or host in known_host:
                rate = known_rate
                break
        _rate_limiters[server_url] = _RateLimiter(rate)
        log.info("Rate limiter for %s: %.1f req/s, %d concurrent", host, rate, _rate_limiters[server_url].max_concurrent)
    return _rate_limiters[server_url]


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
    """Query RDAP for a single domain with rate-limited retries."""
    limiter = _get_rate_limiter(server_url)
    url = f"{server_url}/domain/{domain}"
    rate_limit_hits = 0

    for attempt in range(MAX_RETRIES):
        try:
            async with limiter:
                resp = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)

            if resp.status_code == 404:
                return (domain, None)

            # 4xx client errors (except 429) = permanent, skip immediately
            if resp.status_code in (400, 403, 410):
                return (domain, None)

            if resp.status_code == 429:
                rate_limit_hits += 1
                retry_after = float(resp.headers.get("Retry-After", _backoff(attempt)))
                retry_after = min(retry_after, MAX_BACKOFF)
                # Throttle the limiter so ALL requests to this server slow down
                limiter.throttle()
                if rate_limit_hits >= MAX_429_RETRIES:
                    log.warning("Server %s rate-limiting — skipping %s after %d 429s",
                                server_url, domain, rate_limit_hits)
                    return (domain, None)
                log.debug("429 for %s — waiting %.1fs (hit %d/%d)",
                          domain, retry_after, rate_limit_hits, MAX_429_RETRIES)
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                delay = _backoff(attempt)
                log.warning("HTTP %d for %s — retrying in %.1fs (attempt %d/%d)",
                            resp.status_code, domain, delay, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            return (domain, _parse_registration_date(resp.json()))

        except Exception as exc:
            delay = _backoff(attempt)
            if attempt >= 2:
                log.warning("Error for %s: %s — retrying in %.1fs (attempt %d/%d)",
                            domain, type(exc).__name__, delay, attempt + 1, MAX_RETRIES)
            await asyncio.sleep(delay)

    log.error("Exhausted %d retries for %s — skipping", MAX_RETRIES, domain)
    return (domain, None)


async def process_batch(
    client: httpx.AsyncClient, domains: list[str],
) -> tuple[list[tuple[str, datetime]], list[str]]:
    """Return (found, all_checked) — found has dates, all_checked is every domain we looked up."""
    tasks = []
    skipped_domains: list[str] = []
    for domain in domains:
        server = get_rdap_server(domain)
        if server is None:
            skipped_domains.append(domain)
            continue
        tasks.append(fetch_registration_date(client, domain, server))
    if skipped_domains:
        log.info("Skipped %d domains with no known RDAP server", len(skipped_domains))
    if not tasks:
        return [], skipped_domains
    results = await asyncio.gather(*tasks, return_exceptions=True)
    found = []
    checked = list(skipped_domains)
    for r in results:
        if isinstance(r, Exception):
            log.warning("Unexpected gather exception: %s", r)
            continue
        domain, dt = r
        checked.append(domain)
        if dt is not None:
            found.append((domain, dt))
    return found, checked


async def update_db(
    pool: asyncpg.Pool,
    found: list[tuple[str, datetime]],
    all_checked: list[str],
) -> int:
    """Batch-update: set registered_at for found, rdap_checked_at for all checked."""
    if not all_checked:
        return 0
    now = datetime.now(timezone.utc)
    for attempt in range(5):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if found:
                        await conn.executemany(
                            "UPDATE global_domains SET registered_at = $1 WHERE name = $2",
                            [(dt, domain) for domain, dt in found],
                        )
                    await conn.executemany(
                        "UPDATE global_domains SET rdap_checked_at = $1 WHERE name = $2",
                        [(now, domain) for domain in all_checked],
                    )
            return len(found)
        except Exception as exc:
            delay = _backoff(attempt)
            log.warning("DB write failed: %s — retrying in %.1fs (attempt %d/5)", exc, delay, attempt + 1)
            await asyncio.sleep(delay)
    log.error("DB write failed after 5 attempts — dropping %d results", len(all_checked))
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
                           count(rdap_checked_at) AS checked,
                           count(*) - count(rdap_checked_at) AS remaining
                    FROM global_domains
                    """
                )
        except Exception as e:
            return {
                "running": self.running, "round": self.round_num, "total": 0, "done": 0,
                "checked": 0, "remaining": 0, "pct": 0,
                "total_updated_this_session": self.total_updated,
                "rate": self.rate, "elapsed_min": 0, "crash_count": self.crash_count,
                "error": str(e),
            }
        total, done, checked, remaining = row["total"], row["done"], row["checked"], row["remaining"]
        pct = round(checked / total * 100, 2) if total else 0
        elapsed = time.time() - self.started_at if self.started_at and self.running else 0
        return {
            "running": self.running,
            "round": self.round_num,
            "total": total,
            "done": done,
            "checked": checked,
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
                                "SELECT name FROM global_domains WHERE rdap_checked_at IS NULL LIMIT $1",
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

                all_found: list[tuple[str, datetime]] = []
                all_checked: list[str] = []
                for i in range(0, len(domains), WRITE_BATCH):
                    if not self.running:
                        break
                    chunk = domains[i : i + WRITE_BATCH]
                    found, checked = await process_batch(client, chunk)
                    all_found.extend(found)
                    all_checked.extend(checked)

                written = await update_db(pool, all_found, all_checked)
                self.total_updated += written

                elapsed = time.time() - round_start
                self.rate = len(domains) / elapsed if elapsed > 0 else 0

                self._log(
                    f"Round {self.round_num} done: {written} found / {len(all_checked)} checked / {len(domains)} fetched | "
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
        # Reset rate limiters so throttled rates don't persist across sessions
        _rate_limiters.clear()
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
