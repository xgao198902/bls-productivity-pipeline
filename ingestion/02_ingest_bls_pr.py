# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: BLS Productivity Time Series (`pr/`)
# MAGIC
# MAGIC Mirrors the full contents of https://download.bls.gov/pub/time.series/pr/ into
# MAGIC the bronze landing volume, at `/bls/pr/...`.
# MAGIC
# MAGIC Meant to run as a Workflow job task on a schedule. Every run:
# MAGIC 1. Sends a `User-Agent` header identifying this pipeline and a contact email, per
# MAGIC    BLS's [access policy](https://www.bls.gov/bls/pss.htm) - without it, BLS returns
# MAGIC    403 Forbidden.
# MAGIC 2. Crawls the directory listing live (no hardcoded filenames), so it keeps working
# MAGIC    if BLS adds, renames, or removes files.
# MAGIC 3. Diffs the listing against the `raw_ingestion_manifest` Delta table (see
# MAGIC    `_manifest_common`) and only downloads files that are new or whose size/
# MAGIC    last-modified changed - unchanged files are skipped entirely, so re-running
# MAGIC    this job never reprocesses data it already ingested.
# MAGIC 4. Flags (rather than silently deletes) any previously-seen file that BLS has
# MAGIC    removed from the listing.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "bls_productivity")
dbutils.widgets.text("bronze_volume_name", "raw_landing")
dbutils.widgets.text("contact_email", "")
dbutils.widgets.text("base_url", "https://download.bls.gov/pub/time.series/pr/")
dbutils.widgets.text("request_delay_seconds", "0.5")

catalog_name = dbutils.widgets.get("catalog_name").strip()
bronze_volume_name = dbutils.widgets.get("bronze_volume_name").strip()
contact_email = dbutils.widgets.get("contact_email").strip()
base_url = dbutils.widgets.get("base_url").strip()
request_delay_seconds = float(dbutils.widgets.get("request_delay_seconds").strip() or "0.5")

if not contact_email:
    raise ValueError(
        "Widget 'contact_email' is required. BLS's usage policy "
        "(https://www.bls.gov/bls/pss.htm) says it blocks robots that don't include "
        "contact info; set this widget to an email address you control so it can be "
        "sent in the User-Agent header."
    )

SOURCE = "bls_pr"
VOLUME_ROOT = f"/Volumes/{catalog_name}/bronze/{bronze_volume_name}"
DEST_ROOT = f"{VOLUME_ROOT}/bls/pr"
USER_AGENT = f"bls-productivity-pipeline/1.0 (contact: {contact_email})"
HEADERS = {"User-Agent": USER_AGENT}

# COMMAND ----------

# MAGIC %run ./_manifest_common

# COMMAND ----------

import hashlib
import json
import os
import re
import time
from urllib.parse import urljoin

import requests

# BLS serves an IIS-style directory listing, e.g.:
#   6/4/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br>
# Sub-directories use "<dir>" in place of a size. Confirmed by fetching the live
# listing with a proper User-Agent header.
DIR_ENTRY_RE = re.compile(
    r'(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s+'
    r'(?:(?P<dirmark><dir>)|(?P<size>\d+))\s+<A HREF="(?P<href>[^"]+)">(?P<name>[^<]+)</A>',
    re.IGNORECASE,
)


def fetch_directory_listing(url: str) -> list:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    entries = []
    for m in DIR_ENTRY_RE.finditer(resp.text):
        entries.append(
            {
                "name": m.group("name").strip(),
                "href": m.group("href"),
                "is_dir": m.group("dirmark") is not None,
                "size_bytes": int(m.group("size")) if m.group("size") else None,
                "last_modified": f"{m.group('date')} {m.group('time')}",
            }
        )
    return entries


def crawl_remote_tree(url: str, relative_prefix: str = "") -> list:
    """Recursively walks the directory listing (handles any sub-directories BLS
    might add later, though pr/ is currently flat) and returns a flat list of
    file entries with paths relative to the crawl root."""
    files = []
    for entry in fetch_directory_listing(url):
        relative_path = f"{relative_prefix}{entry['name']}"
        remote_url = urljoin(url, entry["href"])
        if entry["is_dir"]:
            time.sleep(request_delay_seconds)
            sub_url = remote_url if remote_url.endswith("/") else remote_url + "/"
            files.extend(crawl_remote_tree(sub_url, relative_path + "/"))
        else:
            files.append(
                {
                    "relative_path": relative_path,
                    "remote_url": remote_url,
                    "size_bytes": entry["size_bytes"],
                    "last_modified": entry["last_modified"],
                }
            )
    return files


def download_with_retry(url: str, attempts: int = 3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            last_error = e
            if attempt < attempts:
                time.sleep(2 * attempt)
    raise last_error

# COMMAND ----------

manifest_state = load_manifest_state(spark, catalog_name, SOURCE)
remote_files = crawl_remote_tree(base_url)
print(f"Found {len(remote_files)} file(s) at {base_url}")

os.makedirs(DEST_ROOT, exist_ok=True)

now = now_utc()
manifest_rows = []
seen_paths = []
new_count, changed_count, skipped_count, failed_count = 0, 0, 0, 0

for remote in remote_files:
    relative_path = remote["relative_path"]
    seen_paths.append(relative_path)
    existing = manifest_state.get(relative_path)
    dest_path = os.path.join(DEST_ROOT, relative_path)

    must_fetch = needs_fetch(remote["size_bytes"], remote["last_modified"], existing) or not os.path.exists(dest_path)

    if not must_fetch:
        skipped_count += 1
        manifest_rows.append(
            {
                "source": SOURCE,
                "relative_path": relative_path,
                "remote_url": remote["remote_url"],
                "size_bytes": remote["size_bytes"],
                "last_modified": remote["last_modified"],
                "content_hash": existing.get("content_hash"),
                "status": "active",
                "first_seen_at": existing["first_seen_at"],
                "last_seen_at": now,
                "last_ingested_at": existing.get("last_ingested_at"),
            }
        )
        continue

    try:
        content = download_with_retry(remote["remote_url"])
    except requests.RequestException as e:
        print(f"FAILED to download {relative_path} after retries: {e}")
        failed_count += 1
        continue

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(content)

    is_new = existing is None
    new_count += 1 if is_new else 0
    changed_count += 0 if is_new else 1
    print(f"{'Downloaded new' if is_new else 'Re-downloaded changed'} file: {relative_path} ({len(content)} bytes)")

    manifest_rows.append(
        {
            "source": SOURCE,
            "relative_path": relative_path,
            "remote_url": remote["remote_url"],
            "size_bytes": remote["size_bytes"] if remote["size_bytes"] is not None else len(content),
            "last_modified": remote["last_modified"],
            "content_hash": hashlib.sha256(content).hexdigest(),
            "status": "active",
            "first_seen_at": existing["first_seen_at"] if existing else now,
            "last_seen_at": now,
            "last_ingested_at": now,
        }
    )
    time.sleep(request_delay_seconds)

upsert_manifest(spark, catalog_name, manifest_rows)
removed_count = mark_removed(spark, catalog_name, SOURCE, seen_paths)

summary = {
    "source": SOURCE,
    "remote_file_count": len(remote_files),
    "new": new_count,
    "changed": changed_count,
    "skipped_unchanged": skipped_count,
    "failed": failed_count,
    "marked_removed": removed_count,
}
print(json.dumps(summary, indent=2))

if failed_count > 0:
    raise RuntimeError(f"{failed_count} file(s) failed to download - see log above.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary))
