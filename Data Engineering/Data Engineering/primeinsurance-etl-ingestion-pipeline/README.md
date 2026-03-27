# PrimeInsurance — Bronze Ingestion Pipeline

This folder contains the Bronze layer DLT pipeline for the PrimeInsurance Data Intelligence Platform. It ingests raw source files from Unity Catalog Volumes into Delta tables using Auto Loader (`cloudFiles`), with no transformations applied — all data lands as-is for full auditability.

---

## Pipeline Overview

| Table | Format | Source Files | Records |
|---|---|---|---|
| `customers` | CSV | `customers_*.csv` (7 files across regional subdirectories) | ~1,604 raw |
| `claims` | JSON | `claims_*.json` (2 files including root-level) | ~1,000 raw |
| `policy` | CSV | `policy.csv` | ~1,000 raw |
| `sales` | CSV | `*[Ss]ales*.csv` (3 files, mixed casing) | ~4,981 raw (incl. blank padding rows) |
| `cars` | CSV | `cars.csv` | ~2,500 raw |

All tables include two lineage columns added at ingestion:
- `_source_file` — full path of the originating file
- `_load_timestamp` — timestamp when the record was loaded
- `_rescued_data` — any columns that didn't fit the inferred schema (Auto Loader rescue column)

---

## Source Location

```
/Volumes/primeinsurance/bronze/raw_files/autoinsurancedata/
```

Schema checkpoints (required by Auto Loader for schema evolution tracking):

```
/Volumes/primeinsurance/bronze/raw_files/_checkpoints/
```

---

## Design Decisions

**`inferSchema = false`** — All columns land as strings in Bronze. Type casting and validation happen in the Silver layer to avoid silent data loss at ingestion.

**`mergeSchema = true`** — Enabled for multi-file tables (`customers`, `claims`, `sales`) since source files from different regional systems have varying column sets. Columns missing in a given file are padded with `NULL`.

**`pathGlobFilter`** — Each table uses a targeted glob pattern rather than scanning the entire volume. Notable cases:
- `customers_*.csv` and `claims_*.json` — Auto Loader scans all subdirectory levels, so root-level files are picked up without additional config.
- `*[Ss]ales*.csv` — Case-insensitive pattern to handle inconsistent file naming (`Sales_2.csv` vs `sales_1.csv`) across source systems.

---

## Running the Pipeline

- **Single table** — Open the notebook and use `Run file` to preview one table's output.
- **Full pipeline** — Use `Run pipeline` to execute all five Bronze tables end-to-end.
- **Scheduled runs** — Use `Schedule` in the Databricks UI to automate ingestion.

---

## Folder Structure

```
bronze/
├── bronze_pipeline.py       # All 5 Bronze DLT table definitions
└── README.md
```

---

## What's Next

Bronze tables feed directly into the Silver layer (`silver_pipeline.py`), which handles:
- Schema harmonization across multi-source tables (customers, sales)
- Type casting and null standardization
- Data quality checks with quarantine tables for failed records
