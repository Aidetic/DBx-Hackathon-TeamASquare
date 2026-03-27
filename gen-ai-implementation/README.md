# PrimeInsurance — Gen AI Implementation

This folder contains the four Gen AI notebooks built on top of the Gold layer. Each notebook uses the Databricks-hosted LLM (`databricks-gpt-oss-20b`) accessed via the OpenAI-compatible serving endpoint. Together they cover data quality explanation, claims fraud detection, executive reporting, and a RAG-based policy assistant.

---

## Notebooks Overview

| Notebook | Output Table(s) | What It Does |
|---|---|---|
| `designing_the_dq_insight_engine.py` | `silver.dq_issues`, `gold.dq_explanation_report` | Builds a structured DQ issue registry and generates plain-English explanations for each issue |
| `engineering_the_claims_risk_anomaly_engine.py` | `gold.claim_anomaly_explanations` | Scores all 1,000 claims for fraud risk and generates investigation briefs for flagged claims |
| `synthesizing_executive_business_insights.py` | `gold.ai_business_insights` | Aggregates Gold-layer KPIs and generates executive summaries for policy, claims, and customer domains |
| `orchestrating_the_policy_intelligence_assistant.py` | `gold.rag_query_history` | Builds a RAG pipeline over policy documents using FAISS + sentence-transformers for natural language policy lookup |

---

## Model & Infrastructure

- **LLM**: `databricks-gpt-oss-20b` via Databricks Model Serving
- **API**: OpenAI-compatible endpoint (`/serving-endpoints`) using the workspace token for auth
- **Embedding model** (RAG only): `all-MiniLM-L6-v2` via `sentence-transformers`
- **Vector store** (RAG only): FAISS (`faiss-cpu`) — in-memory flat L2 index

The model returns responses as a Python list of content blocks. The `extract_text()` helper function used across notebooks handles this by iterating through blocks and returning the first `type: "text"` block.

---

## Notebook Details

### 1. DQ Insight Engine — `designing_the_dq_insight_engine.py`

Builds a catalog of 14 known data quality issues discovered during exploration, stores them as a structured table, then runs each issue through the LLM to produce a compliance-ready explanation.

**What it creates:**

`primeinsurance.silver.dq_issues` — 14 rows, one per issue, with severity, affected record counts, technical description, and suggested fix.

`primeinsurance.gold.dq_explanation_report` — same 14 rows enriched with an AI-generated explanation per issue structured as: Finding → Business Impact → Root Cause → Remediation Applied → Prevention Recommendation.

**Issues catalogued:**

| Issue ID | Table | Type | Severity |
|---|---|---|---|
| DQ-001 | customers | schema_variation (3 ID column names) | critical |
| DQ-002/003 | customers | schema_variation (Region abbreviations / column rename) | high |
| DQ-004 | customers | column_swap (customers_6) | critical |
| DQ-005 | customers | data_corruption ("terto" typo) | medium |
| DQ-006/007 | customers | missing_column (marital, HHInsurance in customers_2) | high/medium |
| DQ-008 | claims | data_corruption (incident_date 27:00.0 format) | critical |
| DQ-009 | claims | data_corruption ("NULL" string in Claim_Processed_On) | high |
| DQ-010 | claims | unknown_values ("?" in property_damage) | medium |
| DQ-011 | claims | type_mismatch ("NULL" string in vehicle column) | medium |
| DQ-012 | sales | blank_rows (3,132 CSV padding rows) | critical |
| DQ-013 | policy | invalid_value (negative umbrella_limit) | high |
| DQ-014 | customers | duplicate_records (2,001 dupes across 7 files) | critical |

---

### 2. Claims Risk Anomaly Engine — `engineering_the_claims_risk_anomaly_engine.py`

Scores all 1,000 claims using a 5-rule rule-based anomaly detection system, then generates LLM investigation briefs for HIGH and MEDIUM priority claims.

**Scoring rules:**

| Rule | Trigger | Max Points |
|---|---|---|
| `HIGH_AMOUNT` | Z-score > 1.0 on total claim amount (mean $12,652, std $10,744) | 30 |
| `SEVERITY_MISMATCH` | Trivial/Minor damage with above-average claim amount | 25 |
| `MISSING_DOCUMENTATION` | Property damage reported but no police report | 20 |
| `NO_WITNESSES_MULTI_VEHICLE` | 3+ vehicles, 0 witnesses | 15 |
| `INJURY_WITHOUT_PROPERTY` | Bodily injuries reported but no property damage | 10 |

**Priority tiers:** HIGH (≥ 40), MEDIUM (≥ 20), LOW (< 20). Score capped at 100.

**Output table** `primeinsurance.gold.claim_anomaly_explanations` — all 1,000 claims with anomaly score, priority tier, triggered rules, and AI-generated investigation brief (HIGH + MEDIUM only; LOW claims have `ai_investigation_brief = NULL`).

LLM briefs follow the format: Suspicion Summary → Risk Factors → Recommended Actions, grounded in the specific dollar amounts and data points of each claim.

---

### 3. Executive Business Insights — `synthesizing_executive_business_insights.py`

Pulls KPIs from Gold tables across three business domains, then generates a 200–300 word executive summary for each domain using the LLM.

**Domains covered:**

| Domain | Source | Key KPIs |
|---|---|---|
| Policy Portfolio | `dim_policy` | Total policies, premium by tier, umbrella coverage gap, state concentration |
| Claims Performance | `fact_claims` | Rejection rate, exposure by region and coverage tier |
| Customer Profile | `dim_customer` | Regional distribution, default rates, HH insurance, education/marital demographics |

**Output table** `primeinsurance.gold.ai_business_insights` — 3 rows, one per domain, each with the full AI-generated executive summary and the raw KPI JSON used as input.

Also demonstrates Databricks `ai_query()` function for inline SQL-based LLM calls as an alternative pattern.

---

### 4. Policy Intelligence Assistant (RAG) — `orchestrating_the_policy_intelligence_assistant.py`

Builds a retrieval-augmented generation pipeline over the 999 policy records. Converts each policy row to a natural language document, embeds them, indexes with FAISS, and answers natural language questions by retrieving the most relevant policies and passing them as context to the LLM.

**Pipeline steps:**
1. Convert each policy row from `dim_policy` to a natural language string (e.g. _"Policy 100804 is a High-tier auto insurance policy in OH with coverage limits of 500/1000..."_)
2. Encode all 999 documents using `all-MiniLM-L6-v2` → 384-dimensional embeddings
3. Build a FAISS flat L2 index over the embeddings
4. At query time: embed the question → search top-5 → pass retrieved docs as context → LLM generates a grounded answer citing specific policy numbers

**Output table** `primeinsurance.gold.rag_query_history` — logs each query with retrieved policy IDs, confidence scores, LLM answer, and query type classification.

**Test queries run:**
- Specific policy lookup by number
- Highest annual premium policies
- Ohio policies with umbrella coverage
- Policies with $2,000 deductible
- High-tier policy distribution by state

This notebook also adds column-level descriptions (`COMMENT ON COLUMN`) to `dim_policy`, `dim_customer`, and `fact_claims` to support the LLM with structured metadata context.

---

## Output Tables Summary

| Table | Schema | Rows | Populated By |
|---|---|---|---|
| `dq_issues` | silver | 14 | DQ Insight Engine |
| `dq_explanation_report` | gold | 14 | DQ Insight Engine |
| `claim_anomaly_explanations` | gold | 1,000 | Claims Risk Anomaly Engine |
| `ai_business_insights` | gold | 3 | Executive Business Insights |
| `rag_query_history` | gold | 5+ | Policy Intelligence Assistant |

---

## Dependencies

```
openai
sentence-transformers     # RAG notebook only
faiss-cpu                 # RAG notebook only
```

`sentence-transformers` and `faiss-cpu` are installed inline in the RAG notebook via `%pip install`. All other notebooks use the OpenAI SDK which is pre-installed in Databricks Runtime.