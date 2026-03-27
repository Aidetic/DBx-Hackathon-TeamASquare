# PrimeInsurance — Data Exploration & Information Gathering

This folder contains the ad-hoc SQL queries used during the initial data exploration phase of the PrimeInsurance platform. The goal was to understand the raw source data before designing the pipeline — profiling quality, identifying schema inconsistencies, validating relationships, and surfacing data issues that would need to be handled in Bronze and Silver layers.

All queries run against `enqurious_insurance.raw.*` tables.

---

## What Was Explored

The raw data spans 5 entities across 12 source tables:

| Entity | Source Tables |
|---|---|
| Customers | `customers_1` through `customers_7` |
| Policy | `policy` |
| Cars | `cars` |
| Sales | `sales_1`, `sales_2`, `sales_4` |
| Claims | `claims_1`, `claims_2` |

---

## Query Index

| # | Query | Purpose |
|---|---|---|
| 1 | Row count analysis | Record counts across all source tables |
| 2 | Customer schema comparison | Column naming differences across 7 customer files |
| 3 | Null value profiling | Missing value counts for key customer attributes |
| 4 | Region value analysis | Identify abbreviated vs full-name region formats |
| 5 | Education category inspection | Detect typos and inconsistent categorical values |
| 6 | Column swap validation | Confirm Education/Marital_status swap in `customers_6` |
| 7 | Customer union view | Build unified customer dataset across all sources |
| 8 | Customer duplication check | Find overlapping customer IDs across source files |
| 9 | `customers_7` validation | Confirm it acts as the master/consolidated dataset |
| 10 | Policy table profiling | Row counts, nulls, distinct key counts |
| 11 | Policy anomaly detection | Find negative umbrella limits and invalid premiums |
| 12 | Policy → customer integrity | Verify all policies reference a valid customer |
| 13 | Policy → car integrity | Verify all policies reference a valid vehicle |
| 14 | Sales blank row detection | Identify fully null padding rows in CSV files |
| 15 | Sales cleaning validation | Find partially populated but invalid sales rows |
| 16 | Clean sales record counts | Usable row counts after blank row removal |
| 17 | Unsold vehicle detection | Count listings with no `sold_on` date |
| 18 | Inventory aging analysis | Days unsold by region, 60+ and 90+ day buckets |
| 19 | Duplicate sales detection | Transactions appearing across multiple regional files |
| 20 | Car dataset profiling | Null counts and distinct key check for cars |
| 21 | Vehicle attribute distribution | Fuel type, transmission, seats breakdown |
| 22 | Sales → car integrity | Verify all sales reference a valid vehicle |
| 23 | Policy join validation | Sample join across policy, customers_7, and cars |
| 24 | Claims dataset profiling | Row counts and null check for key claim fields |
| 25 | Claim duplication check | Verify `ClaimID` uniqueness |
| 26 | Claims null analysis | Missing value counts across claims fields |
| 27 | Claim timestamp analysis | Inspect raw timestamp format and parse feasibility |
| 28 | Claims relationship validation | Confirm claims link to customers via policy, not directly |

---

## Key Findings

**Customers**
- 7 source files from independent systems — column names differ across all of them (`CustomerID` / `Customer_ID` / `cust_id`, `Region` / `Reg`, `Education` / `Edu`)
- `customers_6` has `Education` and `Marital_status` columns swapped at source
- `customers_5` uses single-letter region abbreviations (`E/W/C/S`) instead of full names
- `customers_5` contains `"terto"` as a typo for `"tertiary"`
- Some customer IDs appear across multiple files — deduplication required
- `customers_7` confirmed as master dataset; exclusive records found here that don't exist in other files

**Policy**
- Policy identifiers are unique — no duplicates
- Referential integrity with both customers and cars is intact
- 1 record found with a negative `umbrella_limit` — violates business logic

**Sales**
- 3 source files contain large numbers of fully blank rows (CSV padding artifacts) — counts are misleading before cleaning
- 164 records have `sold_on IS NULL` — these are genuinely unsold listings, not missing data
- Duplicate `sales_id` values found across regional files — deduplication needed before aggregating revenue
- Multiple vehicles unsold for 60+ and 90+ days across regions — confirmed inventory aging problem

**Cars**
- No duplicate `car_id` values, minimal nulls — reliable reference table
- Numeric columns (`mileage`, `engine`, `max_power`) store values with unit suffixes (e.g. `"23.4 kmpl"`) — string parsing required
- All sales and policy records reference valid car IDs

**Claims**
- Unique `ClaimID` per record — no duplicates
- Claims do not link directly to customers; the join path is `claims → policy → customers`
- `Claim_Logged_On` and `incident_date` timestamps could not be parsed — stored in a corrupted `27:00.0` format, unrecoverable

---

## How These Findings Shaped the Pipeline

| Finding | Where It Was Handled |
|---|---|
| Multi-variant column names in customers | Silver — `COALESCE` across all variants |
| `customers_6` column swap | Silver — swap detected and corrected via value inspection |
| Region abbreviations | Silver — mapped to full names |
| `"terto"` typo | Silver — corrected to `"tertiary"` |
| Customer deduplication | Silver — `ROW_NUMBER` with `customers_7` priority |
| Negative `umbrella_limit` | Silver — quarantined via `expect_or_drop` |
| Sales blank rows | Silver — filtered before quality checks |
| Sales deduplication | Silver — handled via `sales_id` dedup logic |
| Unit suffixes in cars | Silver — `regexp_extract` to strip and cast |
| Corrupted claim timestamps | Silver — retained as `_raw` columns, not parsed |
| Claims → customer join path | Gold — `fact_claims` joins through `policy` |