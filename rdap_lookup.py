import asyncio
import logging
import os
import sys
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
DATABASE_URL = os.environ["DATABASE_URL"]
BATCH_SIZE = 1000            # domains fetched per DB round-trip
WRITE_BATCH = 500            # results buffered before flushing to DB
CONCURRENCY_PER_SERVER = 5   # max parallel requests per RDAP server
MAX_RETRIES = 3
BASE_BACKOFF = 2.0           # seconds, doubled each retry
REQUEST_TIMEOUT = 30.0       # seconds per RDAP request
IANA_BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"

# ---------------------------------------------------------------------------
# RDAP bootstrap — maps TLD -> RDAP base URL
# ---------------------------------------------------------------------------
_tld_to_server: dict[str, str] = {}
_server_semaphores: dict[str, asyncio.Semaphore] = {}


async def load_bootstrap(client: httpx.AsyncClient) -> None:
    """Fetch IANA bootstrap file and build TLD -> RDAP server mapping."""
    log.info("Fetching IANA RDAP bootstrap from %s", IANA_BOOTSTRAP_URL)
    resp = await client.get(IANA_BOOTSTRAP_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    for entry in data.get("services", []):
        tlds, urls = entry
        # Pick the first HTTPS URL
        server_url = next((u for u in urls if u.startswith("https://")), urls[0])
        server_url = server_url.rstrip("/")
        for tld in tlds:
            _tld_to_server[tld.lower()] = server_url

    log.info("Loaded RDAP servers for %d TLDs", len(_tld_to_server))


def get_rdap_server(domain: str) -> str | None:
    """Return the RDAP base URL for a domain's TLD, or None if unknown."""
    parts = domain.rsplit(".", 1)
    if len(parts) < 2:
        return None
    tld = parts[1].lower()
    return _tld_to_server.get(tld)


def _get_semaphore(server_url: str) -> asyncio.Semaphore:
    """Get or create a per-server semaphore for rate limiting."""
    if server_url not in _server_semaphores:
        _server_semaphores[server_url] = asyncio.Semaphore(CONCURRENCY_PER_SERVER)
    return _server_semaphores[server_url]


# ---------------------------------------------------------------------------
# RDAP lookup
# ---------------------------------------------------------------------------
def _parse_registration_date(data: dict) -> datetime | None:
    """Extract registration date from RDAP JSON response."""
    events = data.get("events", [])
    # Primary: look for "registration" event
    for ev in events:
        action = ev.get("eventAction", "").lower()
        if action == "registration":
            try:
                return datetime.fromisoformat(ev["eventDate"].replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue

    # Fallback: some servers use variant names
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
    client: httpx.AsyncClient,
    domain: str,
    server_url: str,
) -> tuple[str, datetime | None]:
    """Query RDAP for a single domain. Returns (domain, registration_date|None)."""
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
                reg_date = _parse_registration_date(resp.json())
                return (domain, reg_date)

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


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------
async def process_batch(
    client: httpx.AsyncClient,
    domains: list[str],
) -> list[tuple[str, datetime]]:
    """Look up registration dates for a batch of domains in parallel."""
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
    # Only return domains where we got an actual date
    return [(domain, dt) for domain, dt in results if dt is not None]


async def update_db(pool: asyncpg.Pool, results: list[tuple[str, datetime]]) -> int:
    """Batch-update registered_at for resolved domains."""
    if not results:
        return 0

    async with pool.acquire() as conn:
        # Use executemany for batch update
        await conn.executemany(
            "UPDATE global_domains SET registered_at = $1 WHERE name = $2",
            [(dt, domain) for domain, dt in results],
        )
    return len(results)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
async def main() -> None:
    log.info("Starting RDAP domain registration date lookup")

    # Connection pool to Neon
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    async with httpx.AsyncClient(
        headers={"Accept": "application/rdap+json, application/json"},
        http2=True,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
    ) as client:
        await load_bootstrap(client)

        total_updated = 0
        round_num = 0

        while True:
            round_num += 1

            # Fetch next batch of domains with no registration date
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT name FROM global_domains WHERE registered_at IS NULL LIMIT $1",
                    BATCH_SIZE,
                )

            if not rows:
                log.info("No more domains to process — all done!")
                break

            domains = [r["name"] for r in rows]
            log.info("Round %d: processing %d domains", round_num, len(domains))

            # Process in sub-batches for write flushing
            all_results: list[tuple[str, datetime]] = []
            for i in range(0, len(domains), WRITE_BATCH):
                chunk = domains[i : i + WRITE_BATCH]
                results = await process_batch(client, chunk)
                all_results.extend(results)

            written = await update_db(pool, all_results)
            total_updated += written

            # Progress report
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT count(*) AS total,
                           count(registered_at) AS done,
                           count(*) - count(registered_at) AS remaining
                    FROM global_domains
                    """
                )

            pct = (row["done"] / row["total"] * 100) if row["total"] else 0
            log.info(
                "Round %d complete: wrote %d | total done: %d/%d (%.1f%%) | remaining: %d",
                round_num,
                written,
                row["done"],
                row["total"],
                pct,
                row["remaining"],
            )

    await pool.close()
    log.info("Finished. Total updated: %d", total_updated)


if __name__ == "__main__":
    asyncio.run(main())
