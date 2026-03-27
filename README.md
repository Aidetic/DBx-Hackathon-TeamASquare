# PrimeInsurance — Data Intelligence Platform

**Team ASquare | Databricks Hackathon**

An end-to-end Data Intelligence Platform built on Databricks Lakehouse for a fictional auto insurance company. The platform ingests fragmented data from 7 regional systems, builds a clean analytical foundation using medallion architecture, and layers Gen AI capabilities on top — covering fraud detection, regulatory compliance, and executive reporting.

---

## The Problem

PrimeInsurance operates across multiple regional systems that were never standardized. The data arrives fragmented, inconsistent, and unreliable:

- Customer records spread across 7 independent files with different column names, swapped columns, typos, and 2,001 duplicate records
- 3,132 blank rows contaminating sales data; 164 genuinely unsold vehicles buried in the noise
- Claims with corrupted timestamps, string literals where nulls should be, and unknown values stored as `"?"`
- 1 policy record with a negative umbrella limit of -$1,000,000

The business consequences: regulatory exposure from unverified customer counts, a claims backlog from poor data quality, and revenue leakage from untracked inventory aging.

---

## Solution Architecture

<svg width="100%" viewBox="0 0 680 1620" xmlns="http://www.w3.org/2000/svg"><defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#888" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker><marker id="arrowR" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#bbb" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker><style>.lbl{font-family:Arial,sans-serif;font-size:11px;fill:#888}.th{font-family:Arial,sans-serif;font-size:14px;font-weight:700}.ts{font-family:Arial,sans-serif;font-size:12px}</style></defs><rect x="20" y="12" width="640" height="40" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/><text class="th" x="340" y="36" text-anchor="middle" dominant-baseline="central" fill="#0C447C">PrimeInsurance — end-to-end lakehouse architecture</text><text class="lbl" x="20" y="72">&#9312; source systems</text><rect x="20" y="80" width="640" height="58" rx="8" fill="#F1EFE8" stroke="#5F5E5A" stroke-width="0.5"/><text class="th" x="340" y="101" text-anchor="middle" dominant-baseline="central" fill="#2C2C2A">6 regional insurance systems</text><text class="ts" x="340" y="120" text-anchor="middle" dominant-baseline="central" fill="#5F5E5A">CSV files (customers, policy, sales, cars)  &#183;  JSON files (claims)</text><line x1="340" y1="138" x2="340" y2="156" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="348" y="150">daily file drop</text><text class="lbl" x="20" y="172">&#9313; governance layer &#8212; Unity Catalog</text><rect x="20" y="180" width="640" height="68" rx="8" fill="#EEEDFE" stroke="#534AB7" stroke-width="0.5"/><text class="th" x="340" y="202" text-anchor="middle" dominant-baseline="central" fill="#3C3489">Unity Catalog + Volumes</text><text class="ts" x="340" y="220" text-anchor="middle" dominant-baseline="central" fill="#534AB7">File storage &#183; access control &#183; cataloging &#183; lineage tracking &#183; discoverability</text><text class="ts" x="340" y="236" text-anchor="middle" dominant-baseline="central" fill="#534AB7">Applies across all layers &#8212; Bronze, Silver, Gold, AI outputs</text><line x1="340" y1="248" x2="340" y2="266" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="20" y="282">&#9314; bronze layer &#8212; raw ingestion</text><rect x="20" y="290" width="640" height="100" rx="8" fill="#FAEEDA" stroke="#BA7517" stroke-width="0.5"/><text class="th" x="340" y="312" text-anchor="middle" dominant-baseline="central" fill="#633806">Bronze Delta tables</text><rect x="36" y="322" width="182" height="56" rx="6" fill="#FAEEDA" stroke="#BA7517" stroke-width="0.5"/><text class="ts" x="127" y="342" text-anchor="middle" dominant-baseline="central" fill="#633806" font-weight="700">Auto Loader (cloudFiles)</text><text class="ts" x="127" y="359" text-anchor="middle" dominant-baseline="central" fill="#854F0B">CSV + JSON ingestion</text><rect x="232" y="322" width="182" height="56" rx="6" fill="#FAEEDA" stroke="#BA7517" stroke-width="0.5"/><text class="ts" x="323" y="342" text-anchor="middle" dominant-baseline="central" fill="#633806" font-weight="700">Schema + mergeSchema</text><text class="ts" x="323" y="359" text-anchor="middle" dominant-baseline="central" fill="#854F0B">inferSchema=false &#183; all strings</text><rect x="428" y="322" width="216" height="56" rx="6" fill="#FAEEDA" stroke="#BA7517" stroke-width="0.5"/><text class="ts" x="536" y="342" text-anchor="middle" dominant-baseline="central" fill="#633806" font-weight="700">Lineage columns</text><text class="ts" x="536" y="359" text-anchor="middle" dominant-baseline="central" fill="#854F0B">_source_file &#183; _load_timestamp</text><line x1="340" y1="390" x2="340" y2="408" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="348" y="402">Spark Declarative Pipelines (DLT)</text><text class="lbl" x="20" y="424">&#9315; silver layer &#8212; data quality &amp; harmonization</text><rect x="20" y="432" width="640" height="180" rx="8" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/><text class="th" x="340" y="454" text-anchor="middle" dominant-baseline="central" fill="#085041">Silver &#8212; validated &amp; standardized tables</text><rect x="36" y="464" width="300" height="136" rx="6" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/><text class="ts" x="186" y="484" text-anchor="middle" dominant-baseline="central" fill="#085041" font-weight="700">Data quality checks (expect_or_drop)</text><text class="ts" x="186" y="502" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; valid_customer_id &#8212; NOT NULL</text><text class="ts" x="186" y="518" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; valid_region &#8212; East/West/Central/South</text><text class="ts" x="186" y="534" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; valid_premium &#8212; policy_premium &gt; 0</text><text class="ts" x="186" y="550" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; schema harmonization &#8212; 3 ID variants</text><text class="ts" x="186" y="566" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; column swap fix &#8212; customers_6</text><text class="ts" x="186" y="582" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#10004; duplicate removal &#8212; customers_7 priority</text><rect x="350" y="464" width="140" height="56" rx="6" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/><text class="ts" x="420" y="488" text-anchor="middle" dominant-baseline="central" fill="#085041" font-weight="700">Valid records</text><text class="ts" x="420" y="505" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">&#8594; silver tables</text><rect x="504" y="464" width="140" height="56" rx="6" fill="#FCEBEB" stroke="#A32D2D" stroke-width="0.5"/><text class="ts" x="574" y="488" text-anchor="middle" dominant-baseline="central" fill="#501313" font-weight="700">Failed records</text><text class="ts" x="574" y="505" text-anchor="middle" dominant-baseline="central" fill="#A32D2D">&#8594; _failed quarantine</text><rect x="350" y="534" width="294" height="66" rx="6" fill="#E1F5EE" stroke="#0F6E56" stroke-width="0.5"/><text class="ts" x="497" y="552" text-anchor="middle" dominant-baseline="central" fill="#085041" font-weight="700">Transformations</text><text class="ts" x="497" y="568" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">type casting &#183; null standardization</text><text class="ts" x="497" y="584" text-anchor="middle" dominant-baseline="central" fill="#0F6E56">days_unsold &#183; unit suffix stripping</text><line x1="340" y1="612" x2="340" y2="630" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="20" y="646">&#9316; gold layer &#8212; star schema + AI outputs</text><rect x="20" y="654" width="640" height="202" rx="8" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/><text class="th" x="340" y="676" text-anchor="middle" dominant-baseline="central" fill="#173404">Gold &#8212; business-ready analytical model</text><rect x="36" y="686" width="300" height="76" rx="6" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/><text class="ts" x="186" y="704" text-anchor="middle" dominant-baseline="central" fill="#173404" font-weight="700">Dimension tables</text><text class="ts" x="186" y="721" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">dim_customer &#183; dim_policy &#183; dim_car</text><text class="ts" x="186" y="737" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">dim_date &#183; dim_region</text><rect x="350" y="686" width="294" height="76" rx="6" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/><text class="ts" x="497" y="704" text-anchor="middle" dominant-baseline="central" fill="#173404" font-weight="700">Fact tables</text><text class="ts" x="497" y="721" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">fact_claims &#183; fact_sales</text><text class="ts" x="497" y="737" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">denormalized for dashboard perf.</text><rect x="36" y="776" width="180" height="68" rx="6" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/><text class="ts" x="126" y="796" text-anchor="middle" dominant-baseline="central" fill="#173404" font-weight="700">KPI aggregates</text><text class="ts" x="126" y="812" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">agg_rejection_by_region</text><text class="ts" x="126" y="828" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">agg_unsold_inventory&#8230;</text><rect x="230" y="776" width="414" height="68" rx="6" fill="#EAF3DE" stroke="#3B6D11" stroke-width="0.5"/><text class="ts" x="437" y="796" text-anchor="middle" dominant-baseline="central" fill="#173404" font-weight="700">AI output tables (written back by Gen AI layer)</text><text class="ts" x="437" y="812" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">dq_explanation_report &#183; claim_anomaly_explanations</text><text class="ts" x="437" y="828" text-anchor="middle" dominant-baseline="central" fill="#3B6D11">ai_business_insights &#183; rag_query_history</text><line x1="340" y1="856" x2="340" y2="874" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="20" y="890">&#9317; gen AI layer &#8212; databricks-gpt-oss-20b</text><rect x="20" y="898" width="640" height="236" rx="8" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/><text class="th" x="340" y="920" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C">Gen AI intelligence layer</text><rect x="36" y="930" width="146" height="192" rx="6" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/><text class="ts" x="109" y="950" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">UC1: DQ insights</text><text class="ts" x="109" y="966" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">engine</text><text class="ts" x="109" y="988" text-anchor="middle" dominant-baseline="central" fill="#993C1D">IN: dq_issues</text><text class="ts" x="109" y="1004" text-anchor="middle" dominant-baseline="central" fill="#993C1D">(14 catalogued issues)</text><text class="ts" x="109" y="1044" text-anchor="middle" dominant-baseline="central" fill="#993C1D">OUT: plain-English</text><text class="ts" x="109" y="1060" text-anchor="middle" dominant-baseline="central" fill="#993C1D">compliance report</text><text class="ts" x="109" y="1092" text-anchor="middle" dominant-baseline="central" fill="#993C1D" opacity="0.7">dq_explanation_report</text><rect x="196" y="930" width="146" height="192" rx="6" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/><text class="ts" x="269" y="950" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">UC2: claims fraud</text><text class="ts" x="269" y="966" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">anomaly engine</text><text class="ts" x="269" y="988" text-anchor="middle" dominant-baseline="central" fill="#993C1D">IN: fact_claims</text><text class="ts" x="269" y="1004" text-anchor="middle" dominant-baseline="central" fill="#993C1D">(5 rules, z-score)</text><text class="ts" x="269" y="1044" text-anchor="middle" dominant-baseline="central" fill="#993C1D">OUT: investigation</text><text class="ts" x="269" y="1060" text-anchor="middle" dominant-baseline="central" fill="#993C1D">briefs HIGH+MED</text><text class="ts" x="269" y="1092" text-anchor="middle" dominant-baseline="central" fill="#993C1D" opacity="0.7">claim_anomaly_</text><text class="ts" x="269" y="1108" text-anchor="middle" dominant-baseline="central" fill="#993C1D" opacity="0.7">explanations</text><rect x="356" y="930" width="146" height="192" rx="6" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/><text class="ts" x="429" y="950" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">UC3: executive</text><text class="ts" x="429" y="966" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">insight engine</text><text class="ts" x="429" y="988" text-anchor="middle" dominant-baseline="central" fill="#993C1D">IN: Gold KPIs</text><text class="ts" x="429" y="1004" text-anchor="middle" dominant-baseline="central" fill="#993C1D">(policy, claims,</text><text class="ts" x="429" y="1020" text-anchor="middle" dominant-baseline="central" fill="#993C1D">customer)</text><text class="ts" x="429" y="1044" text-anchor="middle" dominant-baseline="central" fill="#993C1D">OUT: CEO-ready</text><text class="ts" x="429" y="1060" text-anchor="middle" dominant-baseline="central" fill="#993C1D">summaries x3</text><text class="ts" x="429" y="1092" text-anchor="middle" dominant-baseline="central" fill="#993C1D" opacity="0.7">ai_business_insights</text><rect x="516" y="930" width="128" height="192" rx="6" fill="#FAECE7" stroke="#993C1D" stroke-width="0.5"/><text class="ts" x="580" y="950" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">UC4: policy RAG</text><text class="ts" x="580" y="966" text-anchor="middle" dominant-baseline="central" fill="#4A1B0C" font-weight="700">assistant</text><text class="ts" x="580" y="988" text-anchor="middle" dominant-baseline="central" fill="#993C1D">IN: dim_policy</text><text class="ts" x="580" y="1004" text-anchor="middle" dominant-baseline="central" fill="#993C1D">embeddings</text><text class="ts" x="580" y="1020" text-anchor="middle" dominant-baseline="central" fill="#993C1D">FAISS index</text><text class="ts" x="580" y="1044" text-anchor="middle" dominant-baseline="central" fill="#993C1D">OUT: NL answers</text><text class="ts" x="580" y="1060" text-anchor="middle" dominant-baseline="central" fill="#993C1D">citing policy IDs</text><text class="ts" x="580" y="1092" text-anchor="middle" dominant-baseline="central" fill="#993C1D" opacity="0.7">rag_query_history</text><path d="M20 980 Q4 980 4 854 Q4 728 20 728" fill="none" stroke="#bbb" stroke-width="1" stroke-dasharray="4 3" marker-end="url(#arrowR)"/><text class="lbl" x="6" y="864" text-anchor="middle" transform="rotate(-90,6,864)">outputs written back to Gold</text><line x1="340" y1="1134" x2="340" y2="1152" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="20" y="1168">&#9318; serving layer</text><rect x="20" y="1176" width="640" height="56" rx="8" fill="#E6F1FB" stroke="#185FA5" stroke-width="0.5"/><text class="th" x="340" y="1196" text-anchor="middle" dominant-baseline="central" fill="#0C447C">Databricks SQL Warehouse</text><text class="ts" x="340" y="1214" text-anchor="middle" dominant-baseline="central" fill="#185FA5">fast query execution &#183; concurrent user access &#183; dashboard performance</text><line x1="340" y1="1232" x2="340" y2="1250" stroke="#888" stroke-width="1.5" marker-end="url(#arrow)"/><text class="lbl" x="20" y="1266">&#9319; business users</text><rect x="20" y="1274" width="640" height="56" rx="8" fill="#F1EFE8" stroke="#5F5E5A" stroke-width="0.5"/><text class="th" x="340" y="1294" text-anchor="middle" dominant-baseline="central" fill="#2C2C2A">Analysts &#183; operations teams &#183; executive leadership</text><text class="ts" x="340" y="1312" text-anchor="middle" dominant-baseline="central" fill="#5F5E5A">via Dashboards &#183; Databricks Genie (natural language) &#183; SQL analytics</text><line x1="20" y1="1350" x2="660" y2="1350" stroke="#e0e0e0" stroke-width="1"/><text class="ts" x="20" y="1368" font-weight="700" fill="#333">component map &#8212; tool / feature &#8594; layer &#8594; role</text><rect x="20" y="1378" width="10" height="10" rx="2" fill="#FAEEDA" stroke="#BA7517"/><text class="lbl" x="36" y="1387" fill="#444">Auto Loader &#183; Bronze &#183; CSV/JSON ingestion into raw Delta tables</text><rect x="20" y="1396" width="10" height="10" rx="2" fill="#FAEEDA" stroke="#BA7517"/><text class="lbl" x="36" y="1405" fill="#444">Delta Lake &#183; all layers &#183; ACID transactions, time travel, incremental updates</text><rect x="20" y="1414" width="10" height="10" rx="2" fill="#E1F5EE" stroke="#0F6E56"/><text class="lbl" x="36" y="1423" fill="#444">Spark Declarative Pipelines (DLT) &#183; pipeline &#183; orchestrates Bronze &#8594; Silver &#8594; Gold</text><rect x="20" y="1432" width="10" height="10" rx="2" fill="#E1F5EE" stroke="#0F6E56"/><text class="lbl" x="36" y="1441" fill="#444">expect_or_drop rules &#183; Silver &#183; enforces null, domain, type, and business checks</text><rect x="20" y="1450" width="10" height="10" rx="2" fill="#FCEBEB" stroke="#A32D2D"/><text class="lbl" x="36" y="1459" fill="#444">Quarantine tables &#183; Silver &#183; preserves failed records for compliance review</text><rect x="20" y="1468" width="10" height="10" rx="2" fill="#EAF3DE" stroke="#3B6D11"/><text class="lbl" x="36" y="1477" fill="#444">Star schema (dim + fact) &#183; Gold &#183; business-ready analytical model</text><rect x="20" y="1486" width="10" height="10" rx="2" fill="#EEEDFE" stroke="#534AB7"/><text class="lbl" x="36" y="1495" fill="#444">Unity Catalog &#183; governance &#183; storage, access, lineage, discoverability</text><rect x="20" y="1504" width="10" height="10" rx="2" fill="#FAECE7" stroke="#993C1D"/><text class="lbl" x="36" y="1513" fill="#444">databricks-gpt-oss-20b &#183; Gen AI &#183; LLM for explanations, briefs, summaries</text><rect x="20" y="1522" width="10" height="10" rx="2" fill="#FAECE7" stroke="#993C1D"/><text class="lbl" x="36" y="1531" fill="#444">sentence-transformers + FAISS &#183; Gen AI / RAG &#183; embeddings + vector retrieval</text><rect x="20" y="1540" width="10" height="10" rx="2" fill="#E6F1FB" stroke="#185FA5"/><text class="lbl" x="36" y="1549" fill="#444">Databricks SQL Warehouse &#183; serving &#183; fast, concurrent query execution</text><rect x="20" y="1558" width="10" height="10" rx="2" fill="#E6F1FB" stroke="#185FA5"/><text class="lbl" x="36" y="1567" fill="#444">Databricks Genie &#183; consumption &#183; natural language querying over Gold tables</text><rect x="20" y="1576" width="10" height="10" rx="2" fill="#F1EFE8" stroke="#5F5E5A"/><text class="lbl" x="36" y="1585" fill="#444">Dashboards &#183; consumption &#183; KPI visualisation for business stakeholders</text></svg>

The platform follows an 8-layer medallion architecture on Databricks Lakehouse:

| Layer | What it does |
|---|---|
| Source Systems | 6 regional insurance systems — CSV + JSON files |
| Unity Catalog | Governance, access control, lineage across all layers |
| Bronze | Raw ingestion via Auto Loader — all columns as strings, full lineage |
| Silver | Schema harmonization, DQ checks, quarantine for failed records |
| Gold | Star schema — 4 dims, 2 facts, 5 agg tables + AI output tables |
| Gen AI | 4 use cases: DQ insights, fraud detection, executive summaries, RAG assistant |
| SQL Warehouse | Fast, concurrent query serving for dashboards and Genie |
| Business Users | Analysts, operations, executives via dashboards and natural language |

---

## Repository Structure

```
DBx-Hackathon-TeamASquare/
│
├── README.md                              ← You are here
│
├── Data Engineering/
│   ├── primeinsurance-etl-ingestion-pipeline/
│   │   ├── bronze_pipeline.py             # Auto Loader ingestion — 5 Bronze DLT tables
│   │   └── README.md
│   │
│   ├── primeinsurance-etl-silver-pipeline/
│   │   ├── silver_pipeline.py             # Harmonization + DQ — 5 Silver + 5 quarantine tables
│   │   └── README.md
│   │
│   └── primeinsurance-gold-pipeline/
│       ├── gold_pipeline.py               # Star schema + aggs — 4 dims, 2 facts, 5 agg tables
│       └── README.md
│
├── gen-ai-implementation/
│   ├── designing_the_dQ_insight_ingine.py           # DQ registry + LLM explanations
│   ├── engineering_the_claims_risk_anomaly_engine.py # Fraud scoring + investigation briefs
│   ├── synthesizing_executive_business_insights.py  # KPI aggregation + executive summaries
│   ├── orchestrating_the_policy_intelligence_assistant.py  # RAG policy lookup
│   └── README.md
│
└── information_gathering/
    ├── information_gathering.py           # 28 SQL queries for raw data profiling
    ├── data_testing.py                    # Validation and integrity checks
    └── README.md
```

---

## Data Flow

### Source Data
12 raw source tables across 5 entities, ingested from Unity Catalog Volumes:

| Entity | Files | Format |
|---|---|---|
| Customers | `customers_1` to `customers_7` | CSV |
| Claims | `claims_1`, `claims_2` | JSON |
| Policy | `policy` | CSV |
| Sales | `sales_1`, `sales_2`, `sales_4` | CSV |
| Cars | `cars` | CSV |

### Bronze → Silver → Gold

| Layer | Tables | Purpose |
|---|---|---|
| Bronze | 5 | Raw ingestion — all columns as strings, full lineage |
| Silver | 5 clean + 5 quarantine | Schema harmonization, type casting, DQ enforcement |
| Gold | 4 dims + 2 facts + 5 aggs | Star schema for analytics and dashboards |

### Gen AI Output Tables

| Table | Schema | What It Contains |
|---|---|---|
| `dq_issues` | silver | 14 catalogued data quality issues with severity and fix |
| `dq_explanation_report` | gold | LLM-generated compliance explanations per DQ issue |
| `claim_anomaly_explanations` | gold | All 1,000 claims scored + investigation briefs for flagged ones |
| `ai_business_insights` | gold | Executive summaries for policy, claims, and customer domains |
| `rag_query_history` | gold | Natural language policy queries with retrieved docs and answers |

---

## Key Numbers

| Metric | Value |
|---|---|
| Raw customer records | 3,605 (across 7 files) |
| Unique customers after dedup | 1,604 |
| Clean policy records | 999 |
| Clean claims | 1,000 |
| Claims flagged for fraud investigation | ~300 (HIGH + MEDIUM) |
| Clean sales records | 1,849 |
| Unsold vehicles (revenue at risk) | 164 |
| Blank sales rows removed | 3,132 |
| DQ issues catalogued and explained | 14 |
| Vehicle catalogue records | 2,500 |

---

## Tech Stack

| Component | Technology |
|---|---|
| Platform | Databricks Lakehouse (Unity Catalog) |
| Ingestion | Auto Loader (`cloudFiles`) |
| Pipelines | Delta Live Tables (DLT) |
| Storage | Delta Lake — ACID, time travel |
| Processing | PySpark, Spark SQL |
| Orchestration | Databricks Workflows |
| LLM | `databricks-gpt-oss-20b` via Model Serving |
| Embeddings | `all-MiniLM-L6-v2` (sentence-transformers) |
| Vector Search | FAISS (`faiss-cpu`) |
| Cloud Storage | ADLS / S3 via Unity Catalog Volumes |

---

## Layer-by-Layer Detail

Each layer has its own README with full implementation details:

- **[Bronze README](Data%20Engineering/primeinsurance-etl-ingestion-pipeline/README.md)** — Auto Loader config, glob patterns, schema checkpoint locations, design decisions
- **[Silver README](Data%20Engineering/primeinsurance-etl-silver-pipeline/README.md)** — DQ rules per table, known source data issues, quarantine table structure, transformation logic
- **[Gold README](Data%20Engineering/primeinsurance-gold-pipeline/README.md)** — Star schema diagram, denormalization decisions, derived column reference, pre-agg table purpose
- **[Gen AI README](gen-ai-implementation/README.md)** — LLM integration pattern, fraud scoring rules, RAG pipeline steps, output table schema
- **[Exploration README](information_gathering/README.md)** — 28 query index, findings per entity, how findings shaped pipeline decisions
