# PrimeInsurance — Gold Layer Pipeline

This folder contains the Gold layer DLT pipeline for the PrimeInsurance Data Intelligence Platform. It reads from Silver tables and produces a Star Schema dimensional model — dimension tables, fact tables, and pre-aggregated summary tables that power dashboards and business reporting.

---

## Pipeline Overview

### Dimension Tables

| Table | Source | Records | Description |
|---|---|---|---|
| `dim_customer` | `silver.customers` | 1,604 | Deduplicated customer master from 7 regional sources |
| `dim_policy` | `silver.policy` | 999 | Policy records with derived `policy_csl_tier` label |
| `dim_car` | `silver.cars` | 2,500 | Vehicle catalogue with clean numeric specs |
| `dim_date` | Generated | 4,018 | Calendar spine from 2010-01-01 to 2020-12-31 |

### Fact Tables

| Table | Source | Records | Grain |
|---|---|---|---|
| `fact_claims` | `silver.claims` + `silver.policy` + `silver.customers` | 1,000 | One row per insurance claim |
| `fact_sales` | `silver.sales` + `silver.cars` | 1,849 | One row per car listing |

### Pre-Aggregated Tables

| Table | Built From | Business Use |
|---|---|---|
| `agg_claim_rejection_by_policy_region` | `fact_claims` | Rejection rate by policy type and region — morning standup metric |
| `agg_claim_severity_breakdown` | `fact_claims` | Claim counts and financials by incident severity — claims adjuster view |
| `agg_customer_count_by_region` | `dim_customer` | Deduplicated customer count by region — regulatory compliance number |
| `agg_unsold_inventory_by_model_region` | `fact_sales` | Aging unsold cars with revenue at risk — revenue leakage tracking |
| `agg_policy_premium_by_coverage_tier` | `dim_policy` + `fact_claims` | Premium revenue and claim exposure by coverage tier — weekly leadership metric |

---

## Star Schema

```
                    dim_date
                       │
         dim_customer──┤
               │       │
dim_policy ────┼── fact_claims
      │        │
   dim_car ────┴── fact_sales
```

`dim_date` joins on `policy_bind_date` (policy) and `ad_placed_on` (sales).
`incident_date` in claims is corrupted at source and cannot be joined on `dim_date`.

---

## Key Design Decisions

**Denormalization in fact tables**
`fact_claims` carries `policy_csl`, `policy_csl_tier`, `region`, and `state` directly, avoiding repeated 3-table joins on every dashboard query. `fact_sales` carries `car_name`, `car_model`, `fuel`, `transmission`, and `seats` from `dim_car` for the same reason.

**`policy_csl_tier` derivation**
`dim_policy` and `fact_claims` both derive a human-readable coverage tier from the raw `policy_csl` value:

| `policy_csl` | `policy_csl_tier` |
|---|---|
| `100/300` | Low |
| `250/500` | Medium |
| `500/1000` | High |

**`total_claim_amount` in `fact_claims`**
Computed as `injury + property + COALESCE(vehicle, 0)`. Vehicle amount is `NULL` for 29 valid claims where damage wasn't assessed — coalesced to `0` so the sum is never `NULL`.

**`revenue_at_risk` in `fact_sales`**
Populated only for unsold records (`sold_on IS NULL`). `NULL` for sold cars. 164 unsold listings represent the core revenue leakage metric for the platform.

**`days_unsold` in `fact_sales`**
Computed in Silver, carried through as-is. `NULL` for sold records, days since listing date for active unsold listings.

**Pre-aggregated tables**
Refreshed on every pipeline run. Dashboards query these instead of scanning full fact tables, keeping query times predictable regardless of data volume growth.

---

## Derived Columns Reference

| Column | Table | Logic |
|---|---|---|
| `policy_csl_tier` | `dim_policy`, `fact_claims` | Low / Medium / High mapped from `policy_csl` |
| `is_rejected` | `fact_claims` | `1` if `claim_rejected = 'Y'`, else `0` |
| `total_claim_amount` | `fact_claims` | `injury + property + COALESCE(vehicle, 0)` |
| `is_sold` | `fact_sales` | `1` if `sold_on IS NOT NULL`, else `0` |
| `revenue_at_risk` | `fact_sales` | `original_selling_price` for unsold, `NULL` for sold |
| `listing_year` / `listing_month` | `fact_sales` | Extracted from `ad_placed_on` |

---

## Folder Structure

```
primeinsurance-gold-pipeline/
├── exploration/
├── transformation/
      ├──primeinsurance_gold_pipeline.py
├── utilities/
└── README.md
```

---

## What Comes Before

Gold reads exclusively from Silver tables (`primeinsurance.silver.*`). All type casting, null handling, and quality enforcement happens in Silver — Gold assumes clean input.