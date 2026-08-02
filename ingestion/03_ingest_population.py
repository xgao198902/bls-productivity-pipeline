# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: Data USA Population API
# MAGIC
# MAGIC Pulls the population time series from the Data USA API and lands it as raw JSON
# MAGIC in the same bronze volume as the BLS files, at `/population/population.json`.
# MAGIC
# MAGIC Since this is a single JSON response rather than a directory of files, "don't
# MAGIC reprocess anything already ingested" is implemented via a content hash: the
# MAGIC response is always fetched (there's no cheap listing/HEAD to check first), but the
# MAGIC file on the volume is only overwritten - and the manifest's `last_ingested_at`
# MAGIC only bumped - if the content actually changed since the last run.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "bls_productivity")
dbutils.widgets.text("bronze_volume_name", "raw_landing")
dbutils.widgets.text("contact_email", "")
dbutils.widgets.text(
    "api_url",
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population",
)

catalog_name = dbutils.widgets.get("catalog_name").strip()
bronze_volume_name = dbutils.widgets.get("bronze_volume_name").strip()
contact_email = dbutils.widgets.get("contact_email").strip()
api_url = dbutils.widgets.get("api_url").strip()

SOURCE = "population"
RELATIVE_PATH = "population.json"
VOLUME_ROOT = f"/Volumes/{catalog_name}/bronze/{bronze_volume_name}"
DEST_PATH = f"{VOLUME_ROOT}/population/{RELATIVE_PATH}"
USER_AGENT = "bls-productivity-pipeline/1.0" + (f" (contact: {contact_email})" if contact_email else "")
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# COMMAND ----------

# MAGIC %run ./_manifest_common

# COMMAND ----------

import hashlib
import json
import os

import requests

os.makedirs(os.path.dirname(DEST_PATH), exist_ok=True)

resp = requests.get(api_url, headers=HEADERS, timeout=60)
resp.raise_for_status()
content = resp.content
content.decode("utf-8")  # fail fast if the API ever returns something non-JSON/non-UTF-8
content_hash = hashlib.sha256(content).hexdigest()

manifest_state = load_manifest_state(spark, catalog_name, SOURCE)
existing = manifest_state.get(RELATIVE_PATH)
now = now_utc()
changed = existing is None or existing.get("content_hash") != content_hash

if changed:
    with open(DEST_PATH, "wb") as f:
        f.write(content)
    print(f"{'Wrote new' if existing is None else 'Overwrote changed'} file: {DEST_PATH} ({len(content)} bytes)")
else:
    print(f"No change since last run, left file as-is: {DEST_PATH}")

manifest_rows = [
    {
        "source": SOURCE,
        "relative_path": RELATIVE_PATH,
        "remote_url": api_url,
        "size_bytes": len(content),
        "last_modified": None,
        "content_hash": content_hash,
        "status": "active",
        "first_seen_at": existing["first_seen_at"] if existing else now,
        "last_seen_at": now,
        "last_ingested_at": now if changed else existing.get("last_ingested_at"),
    }
]
upsert_manifest(spark, catalog_name, manifest_rows)

summary = {"source": SOURCE, "changed": changed, "bytes": len(content), "path": DEST_PATH}
print(json.dumps(summary, indent=2))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary))
