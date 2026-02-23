import asyncio
import logging
import os
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
DATABASE_URL = os.environ["DATABASE_URL"]
BATCH_SIZE = 1000
WRITE_BATCH = 500
CONCURRENCY_PER_SERVER = 5
MAX_RETRIES = 3
BASE_BACKOFF = 2.0
REQUEST_TIMEOUT = 30.0
IANA_BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"

# ---------------------------------------------------------------------------
# RDAP bootstrap
# ---------------------------------------------------------------------------
_tld_to_server: dict[str, str] = {}
_server_semaphores: dict[str, asyncio.Semaphore] = {}


async def load_bootstrap(client: httpx.AsyncClient) -> None:
    log.info("Fetching IANA RDAP bootstrap from %s", IANA_BOOTSTRAP_URL)
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


def get_rdap_server(domain: str) -> str | None:
    parts = domain.rsplit(".", 1)
    if len(parts) < 2:
        return None
    return _tld_to_server.get(parts[1].lower())


def _get_semaphore(server_url: str) -> asyncio.Semaphore:
    if server_url not in _server_semaphores:
        _server_semaphores[server_url] = asyncio.Semaphore(CONCURRENCY_PER_SERVER)
    return _server_semaphores[server_url]


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
    sem = _get_semaphore(server_url)
    url = f"{server_url}/domain/{domain}"
    for attempt in range(MAX_RETRIES):
        async with sem:
            try:
                resp = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
                if resp.status_code == 404:
                    return (domain, None)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", BASE_BACKOFF * (2 ** attempt)))
                    log.warning("429 from %s for %s — backing off %.1fs", server_url, domain, retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return (domain, _parse_registration_date(resp.json()))
            except httpx.TimeoutException:
                log.warning("Timeout for %s (attempt %d/%d)", domain, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(BASE_BACKOFF * (2 ** attempt))
            except httpx.HTTPStatusError as exc:
                log.warning("HTTP %d for %s (attempt %d/%d)", exc.response.status_code, domain, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(BASE_BACKOFF * (2 ** attempt))
            except httpx.HTTPError as exc:
                log.warning("Network error for %s: %s (attempt %d/%d)", domain, exc, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(BASE_BACKOFF * (2 ** attempt))
    log.error("Failed all %d attempts for %s — skipping", MAX_RETRIES, domain)
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
    results = await asyncio.gather(*tasks)
    return [(domain, dt) for domain, dt in results if dt is not None]


async def update_db(pool: asyncpg.Pool, results: list[tuple[str, datetime]]) -> int:
    if not results:
        return 0
    async with pool.acquire() as conn:
        await conn.executemany(
            "UPDATE global_domains SET registered_at = $1 WHERE name = $2",
            [(dt, domain) for domain, dt in results],
        )
    return len(results)


# ---------------------------------------------------------------------------
# Worker — controllable background task
# ---------------------------------------------------------------------------
class RDAPWorker:
    def __init__(self) -> None:
        self.running = False
        self.task: asyncio.Task | None = None
        self.round_num = 0
        self.total_updated = 0
        self.started_at: float | None = None
        self.logs: deque[str] = deque(maxlen=200)
        self.rate: float = 0.0  # domains/sec
        self._pool: asyncpg.Pool | None = None

    def _log(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.logs.append(entry)
        log.info(msg)

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
        return self._pool

    async def get_progress(self) -> dict:
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
        }

    async def _run(self) -> None:
        self._log("Worker starting")
        self.started_at = time.time()
        pool = await self._get_pool()

        async with httpx.AsyncClient(
            headers={"Accept": "application/rdap+json, application/json"},
            http2=True,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        ) as client:
            if not _tld_to_server:
                await load_bootstrap(client)

            while self.running:
                self.round_num += 1
                round_start = time.time()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT name FROM global_domains WHERE registered_at IS NULL LIMIT $1",
                        BATCH_SIZE,
                    )

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
                    f"Round {self.round_num} done: wrote {written} | "
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
        self.task = asyncio.create_task(self._safe_run())
        return True

    async def _safe_run(self) -> None:
        try:
            await self._run()
        except Exception as e:
            self._log(f"Worker crashed: {e}")
            log.exception("Worker crashed")
        finally:
            self.running = False

    def stop(self) -> bool:
        if not self.running:
            return False
        self.running = False
        self._log("Stop requested — finishing current batch")
        return True


worker = RDAPWorker()
