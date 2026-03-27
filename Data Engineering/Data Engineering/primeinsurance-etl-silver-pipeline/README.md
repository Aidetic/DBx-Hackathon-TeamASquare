# PrimeInsurance — Silver Transformation Pipeline

This folder contains the Silver layer DLT pipeline for the PrimeInsurance Data Intelligence Platform. It reads from Bronze Delta tables, applies schema harmonization, type casting, null standardization, and data quality checks — producing clean Silver tables alongside quarantine tables for any records that fail validation.

---

## Pipeline Overview

Each entity produces two tables: a clean Silver table and a `_failed` quarantine table.

| Silver Table | Quarantine Table | Source | Records (clean) | Key Transformations |
|---|---|---|---|---|
| `customers` | `customers_failed` | `bronze.customers` | ~1,604 | Multi-source column unification, region normalization, customers_6 column swap fix, deduplication |
| `claims` | `claims_failed` | `bronze.claims` | ~1,000 | String-to-float casts, `"NULL"`/`"?"` standardization, corrupted date handling |
| `policy` | `policy_failed` | `bronze.policy` | ~999 | Numeric type casting, date parsing |
| `sales` | `sales_failed` | `bronze.sales` | ~1,849 | Blank row removal, timestamp parsing, `days_unsold` derivation |
| `cars` | `cars_failed` | `bronze.cars` | ~2,500 | Unit suffix stripping (`kmpl`, `CC`, `bhp`), numeric extraction |

---

## Quality Rules per Table

### customers
| Rule | Condition |
|---|---|
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_region` | `region IN ('East','West','Central','South','North')` |
| `valid_marital` | `marital IN ('single','married','divorced') OR marital IS NULL` |
| `valid_education` | `education IN ('primary','secondary','tertiary','NA') OR education IS NULL` |

### claims
| Rule | Condition |
|---|---|
| `valid_claim_id` | `claim_id IS NOT NULL` |
| `valid_policy_id` | `policy_id IS NOT NULL` |
| `valid_rejected` | `claim_rejected IN ('Y','N')` |
| `valid_injury` | `injury_amount IS NOT NULL` |
| `valid_property` | `property_amount IS NOT NULL` |

> `vehicle_amount` NULLs are **not** a quality failure — 29 confirmed valid claims where vehicle damage was not assessed.

### policy
| Rule | Condition |
|---|---|
| `valid_policy_number` | `policy_number IS NOT NULL` |
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_car_id` | `car_id IS NOT NULL` |
| `valid_premium` | `policy_annual_premium > 0` |
| `valid_umbrella` | `umbrella_limit >= 0` |

> 1 record quarantined for `umbrella_limit = -1000000`.

### sales
| Rule | Condition |
|---|---|
| `valid_sales_id` | `sales_id IS NOT NULL` |
| `valid_ad_placed` | `ad_placed_on IS NOT NULL` |
| `valid_car_id` | `car_id IS NOT NULL` |

> 3,132 completely blank rows (CSV padding artifacts) are dropped before quality checks — these are not quarantined.

### cars
| Rule | Condition |
|---|---|
| `valid_car_id` | `car_id IS NOT NULL` |
| `valid_seats` | `seats > 0` |

---

## Known Source Data Issues

These are handled in the pipeline — documented here for transparency.

**customers**
- Three different ID column names across 7 source files (`CustomerID`, `Customer_ID`, `cust_id`) — unified via `COALESCE`.
- `customers_1` uses `Reg` instead of `Region`; `customers_5` uses single-letter abbreviations (`E/W/C/S`).
- `customers_6` has `Education` and `Marital_status` columns swapped at source — detected and corrected.
- `customers_2` uses `City_in_state` instead of `City`.
- `customers_5` contains `"terto"` as a typo for `"tertiary"`.
- `customers_7` is the master file and takes deduplication priority.

**claims**
- `incident_date` and `Claim_Logged_On` are corrupted at source (`27:00.0` minute-fragment format) — unrecoverable. Retained as `_raw` columns for audit only.
- `property_damage` and `police_report_available` use `"?"` for unknown — converted to `NULL`.
- `Claim_Processed_On` contains literal string `"NULL"` — converted to actual `NULL`.

**sales**
- Source CSVs contain 3,132 blank padding rows with all columns `NULL` — filtered out before processing.
- `sold_on IS NULL` means genuinely unsold (164 records) — not a data error.

**cars**
- `mileage`, `engine`, `max_power` store values with unit suffixes (e.g. `"23.4 kmpl"`, `"1248 CC"`) — numeric extracted via regex.
- `torque` has inconsistent formats across sources — kept as `torque_raw` string.

---

## Quarantine Tables

Every Silver table has a corresponding `_failed` table in the same schema. Each failed record includes a `_rejection_reason` column with a comma-separated list of failed checks, making it straightforward to investigate and reprocess.

```
primeinsurance.silver.customers_failed
primeinsurance.silver.claims_failed
primeinsurance.silver.policy_failed
primeinsurance.silver.sales_failed
primeinsurance.silver.cars_failed
```

---

## Folder Structure

```
silver/
├── silver_pipeline.py       # All 5 Silver DLT table + quarantine definitions
└── README.md
```

---

## What's Next

Silver tables feed into the Gold layer (`gold_pipeline.py`), which handles:
- Dimensional modeling (dim/fact structure)
- Denormalization for dashboard performance
- Pre-aggregated tables for reporting
