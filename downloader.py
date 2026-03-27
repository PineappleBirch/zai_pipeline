import asyncio
import aiohttp
import os
import logging
from datetime import datetime
from io import BytesIO

import asyncpg
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Config (all from env) ---
BASE_URL = "https://obcan.justice.sk/pilot/api/ress-isu-service"
GCS_BUCKET = os.environ["GCS_BUCKET"]           # e.g. zakonai-legal-archive
GCS_PREFIX = os.environ.get("GCS_PREFIX", "sk/case-law")
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ["DB_PASSWORD"]
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))
DELAY = float(os.environ.get("DELAY", "0.3"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
PAGE_SIZE = 1000

# Rate limit config
RATE_LIMIT_PAUSE = 300
RATE_LIMIT_THRESHOLD = 10
RATE_LIMIT_REDUCED_WORKERS = 3

FILTERS = {
    "formaRozhodnutiaFacetFilter": ["Rozsudok", "Trestný rozkaz"],
    "vydaniaOd": "1980-01-01",
    "vydaniaDo": datetime.now().strftime("%Y-%m-%d"),
}


# --- DB setup ---
async def init_db(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            guid TEXT PRIMARY KEY,
            forma TEXT,
            sud TEXT,
            sudca TEXT,
            spisova_znacka TEXT,
            datum_vydania TEXT,
            ecli TEXT,
            oblast TEXT,
            pod_oblast TEXT,
            dokument_name TEXT,
            dokument_size INTEGER,
            dokument_url TEXT,
            gcs_path TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS download_log (
            id SERIAL PRIMARY KEY,
            run_date DATE,
            total_collected INTEGER,
            total_downloaded INTEGER,
            total_failed INTEGER,
            duration_sec INTEGER,
            status TEXT
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS collection_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_page INTEGER DEFAULT 0,
            num_found INTEGER DEFAULT 0,
            completed BOOLEAN DEFAULT FALSE
        )
    """)
    await conn.execute("""
        INSERT INTO collection_state (id) VALUES (1)
        ON CONFLICT (id) DO NOTHING
    """)


# --- Step 1: Collect GUIDs ---
async def collect_guids(session, pool):
    async with pool.acquire() as conn:
        state = await conn.fetchrow("SELECT last_page, num_found, completed FROM collection_state WHERE id=1")
    last_page, num_found, completed = state["last_page"], state["num_found"], state["completed"]

    if completed:
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM decisions")
        log.info(f"GUID collection already complete — {total} GUIDs in DB")
        return total

    page = last_page
    total_collected = page * PAGE_SIZE
    log.info(f"Starting GUID collection from page {page}...")

    while True:
        params = {**FILTERS, "page": page, "size": PAGE_SIZE}
        async with session.get(f"{BASE_URL}/v1/rozhodnutie", params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            data = await r.json()

        decisions = data.get("rozhodnutieList", [])
        if not decisions:
            break

        rows = []
        for d in decisions:
            guid = d.get("guid")
            if not guid:
                continue
            rows.append((
                guid,
                d.get("formaRozhodnutia"),
                d.get("sud", {}).get("nazov"),
                d.get("sudca", {}).get("meno"),
                d.get("spisovaZnacka"),
                d.get("datumVydania"),
                d.get("ecli"),
            ))

        async with pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO decisions (guid, forma, sud, sudca, spisova_znacka, datum_vydania, ecli)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (guid) DO NOTHING
            """, rows)
            await conn.execute(
                "UPDATE collection_state SET last_page=$1, num_found=$2 WHERE id=1",
                page + 1, num_found
            )

        total_collected += len(decisions)
        num_found = data.get("numFound", 0)
        log.info(f"Page {page} | collected {total_collected}/{num_found}")

        if len(decisions) < PAGE_SIZE:
            break

        page += 1
        await asyncio.sleep(DELAY)

    async with pool.acquire() as conn:
        await conn.execute("UPDATE collection_state SET completed=TRUE WHERE id=1")
    log.info(f"GUID collection done. Total: {total_collected}")
    return total_collected


# --- Rate limit state ---
class RateLimitTracker:
    def __init__(self):
        self.consecutive = 0
        self.lock = asyncio.Lock()
        self.paused = False
        self.current_workers = MAX_WORKERS

    async def record_429(self):
        async with self.lock:
            self.consecutive += 1
            log.warning(f"Rate limit hit ({self.consecutive} consecutive)")
            if self.consecutive >= RATE_LIMIT_THRESHOLD:
                self.paused = True

    async def record_success(self):
        async with self.lock:
            self.consecutive = 0

    async def wait_if_paused(self):
        if self.paused:
            log.warning(f"Too many rate limit errors — pausing {RATE_LIMIT_PAUSE}s and reducing workers to {RATE_LIMIT_REDUCED_WORKERS}")
            await asyncio.sleep(RATE_LIMIT_PAUSE)
            async with self.lock:
                self.paused = False
                self.consecutive = 0
                self.current_workers = RATE_LIMIT_REDUCED_WORKERS
            log.info("Resuming with reduced concurrency")

rate_tracker = RateLimitTracker()


# --- Step 2: Download PDFs ---
async def fetch_and_download(session, semaphore, pool, gcs_bucket, guid):
    async with semaphore:
        await rate_tracker.wait_if_paused()

        # Get full decision detail
        try:
            async with session.get(f"{BASE_URL}/v1/rozhodnutie/{guid}", timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status in (429, 503):
                    await rate_tracker.record_429()
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE decisions SET attempts=attempts+1, status='failed', updated_at=NOW() WHERE guid=$1",
                            guid
                        )
                    return False
                detail = await r.json()
                await rate_tracker.record_success()
        except Exception as e:
            log.error(f"Failed to fetch detail for {guid}: {e}")
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE decisions SET attempts=attempts+1, status='failed', updated_at=NOW() WHERE guid=$1",
                    guid
                )
            return False

        dokument = detail.get("dokument")
        if not dokument or not dokument.get("url"):
            log.warning(f"No document URL for {guid}")
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE decisions SET status='dead', updated_at=NOW() WHERE guid=$1",
                    guid
                )
            return False

        oblast = ", ".join(detail.get("oblast", []))
        pod_oblast = ", ".join(detail.get("podOblast", []))
        doc_url = dokument["url"]
        doc_name = dokument.get("name", "")
        doc_size = dokument.get("size", 0)

        year = (detail.get("datumVydania") or "0000")[-4:]
        guid_short = guid.split(":")[-1]
        gcs_path = f"{GCS_PREFIX}/{year}/{guid_short}.pdf"

        # Download PDF
        try:
            async with session.get(doc_url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status in (429, 503):
                    await rate_tracker.record_429()
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE decisions SET attempts=attempts+1, status='failed', updated_at=NOW() WHERE guid=$1",
                            guid
                        )
                    return False
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                content = await r.read()
                await rate_tracker.record_success()
        except Exception as e:
            log.error(f"Failed to download PDF for {guid}: {e}")
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE decisions SET attempts=attempts+1, status='failed', updated_at=NOW() WHERE guid=$1",
                    guid
                )
            return False

        # Upload to GCS (blocking call — run in executor)
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _upload_to_gcs, gcs_bucket, gcs_path, content)
        except Exception as e:
            log.error(f"Failed to upload to GCS for {guid}: {e}")
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE decisions SET attempts=attempts+1, status='failed', updated_at=NOW() WHERE guid=$1",
                    guid
                )
            return False

        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE decisions SET
                    oblast=$1, pod_oblast=$2, dokument_name=$3, dokument_size=$4,
                    dokument_url=$5, gcs_path=$6, status='done', updated_at=NOW()
                WHERE guid=$7
            """, oblast, pod_oblast, doc_name, doc_size, doc_url, gcs_path, guid)

        await asyncio.sleep(DELAY)
        return True


def _upload_to_gcs(bucket, gcs_path, content):
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(BytesIO(content), content_type="application/pdf")


async def download_all(session, pool, gcs_bucket):
    log.info("Starting downloads...")

    async with pool.acquire() as conn:
        total = await conn.fetchval("""
            SELECT COUNT(*) FROM decisions
            WHERE status != 'done' AND status != 'dead' AND attempts < $1
        """, MAX_RETRIES)
    log.info(f"Pending downloads: {total}")

    semaphore = asyncio.Semaphore(MAX_WORKERS)
    BATCH_SIZE = 5000
    done = 0
    failed = 0
    progress_every = 100
    last_guid = ""

    while True:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT guid FROM decisions
                WHERE status != 'done' AND status != 'dead' AND attempts < $1
                    AND guid > $2
                ORDER BY guid
                LIMIT $3
            """, MAX_RETRIES, last_guid, BATCH_SIZE)

        if not rows:
            break

        guids = [r["guid"] for r in rows]
        last_guid = guids[-1]

        async def tracked(guid):
            nonlocal done, failed
            result = await fetch_and_download(session, semaphore, pool, gcs_bucket, guid)
            if result:
                done += 1
            else:
                failed += 1
            if (done + failed) % progress_every == 0:
                pct = (done + failed) / total * 100
                log.info(f"[{done + failed}/{total}] {pct:.1f}% | done={done} | failed={failed}")

        await asyncio.gather(*[tracked(guid) for guid in guids])

    log.info(f"Downloads complete. Done: {done} | Failed: {failed}")
    return done, failed


# --- Main ---
async def main():
    t_start = datetime.now()

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(GCS_BUCKET)

    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        min_size=2, max_size=MAX_WORKERS + 2
    )
    try:
        async with pool.acquire() as conn:
            await init_db(conn)

        async with aiohttp.ClientSession() as session:
            total_collected = await collect_guids(session, pool)
            done, failed = await download_all(session, pool, gcs_bucket)

        duration = int((datetime.now() - t_start).total_seconds())
        status = "success" if failed == 0 else "partial"
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO download_log (run_date, total_collected, total_downloaded, total_failed, duration_sec, status)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, datetime.now().date(), total_collected, done, failed, duration, status)

        log.info(f"Pipeline done in {duration}s")
    finally:
        await pool.close()


asyncio.run(main())