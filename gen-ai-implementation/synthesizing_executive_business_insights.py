# Databricks notebook source
from pyspark.sql import functions as F
import json
policy_kpis = {}
policy_kpis["total_policies"] = spark.table("primeinsurance.gold.dim_policy").count()
tier_data = spark.sql("""
    SELECT policy_csl_tier, COUNT(*) as count, 
           ROUND(AVG(policy_annual_premium), 2) as avg_premium,
           ROUND(SUM(policy_annual_premium), 2) as total_revenue
    FROM primeinsurance.gold.dim_policy
    GROUP BY policy_csl_tier ORDER BY policy_csl_tier
""").collect()
policy_kpis["by_tier"] = [r.asDict() for r in tier_data]
state_data = spark.sql("""
    SELECT policy_state, COUNT(*) as count
    FROM primeinsurance.gold.dim_policy
    GROUP BY policy_state ORDER BY count DESC
""").collect()
policy_kpis["by_state"] = [r.asDict() for r in state_data]
umbrella = spark.sql("""
    SELECT 
        SUM(CASE WHEN umbrella_limit > 0 THEN 1 ELSE 0 END) as with_umbrella,
        SUM(CASE WHEN umbrella_limit = 0 THEN 1 ELSE 0 END) as without_umbrella,
        ROUND(AVG(CASE WHEN umbrella_limit > 0 THEN umbrella_limit END), 0) as avg_umbrella_amount
    FROM primeinsurance.gold.dim_policy
""").collect()[0].asDict()
policy_kpis["umbrella"] = umbrella
deductible = spark.sql("""
    SELECT policy_deductable, COUNT(*) as count
    FROM primeinsurance.gold.dim_policy
    GROUP BY policy_deductable ORDER BY policy_deductable
""").collect()
policy_kpis["by_deductible"] = [r.asDict() for r in deductible]
print("=== DOMAIN 1: Policy Portfolio KPIs ===")
print(json.dumps(policy_kpis, indent=2, default=str))
claims_kpis = {}
claims_kpis["total_claims"] = spark.table("primeinsurance.gold.fact_claims").count()
rejection = spark.sql("""
    SELECT 
        SUM(is_rejected) as total_rejected,
        COUNT(*) - SUM(is_rejected) as total_approved,
        ROUND(SUM(is_rejected) * 100.0 / COUNT(*), 1) as rejection_rate_pct,
        ROUND(SUM(total_claim_amount), 2) as total_exposure,
        ROUND(AVG(total_claim_amount), 2) as avg_claim_amount
    FROM primeinsurance.gold.fact_claims
""").collect()[0].asDict()
claims_kpis["overall"] = rejection
severity = spark.sql("""
    SELECT incident_severity, COUNT(*) as count,
           SUM(is_rejected) as rejected,
           ROUND(AVG(total_claim_amount), 2) as avg_amount
    FROM primeinsurance.gold.fact_claims
    GROUP BY incident_severity
""").collect()
claims_kpis["by_severity"] = [r.asDict() for r in severity]
by_region = spark.sql("""
    SELECT region, COUNT(*) as count,
           SUM(is_rejected) as rejected,
           ROUND(SUM(is_rejected) * 100.0 / COUNT(*), 1) as rejection_rate,
           ROUND(SUM(total_claim_amount), 2) as total_exposure
    FROM primeinsurance.gold.fact_claims
    WHERE region IS NOT NULL
    GROUP BY region ORDER BY rejection_rate DESC
""").collect()
claims_kpis["by_region"] = [r.asDict() for r in by_region]
by_tier_claims = spark.sql("""
    SELECT policy_csl_tier, COUNT(*) as count,
           ROUND(SUM(is_rejected) * 100.0 / COUNT(*), 1) as rejection_rate
    FROM primeinsurance.gold.fact_claims
    WHERE policy_csl_tier IS NOT NULL
    GROUP BY policy_csl_tier
""").collect()
claims_kpis["by_coverage_tier"] = [r.asDict() for r in by_tier_claims]

print("\n=== DOMAIN 2: Claims Performance KPIs ===")
print(json.dumps(claims_kpis, indent=2, default=str))
customer_kpis = {}
customer_kpis["total_customers"] = spark.table("primeinsurance.gold.dim_customer").count()

cust_region = spark.sql("""
    SELECT region, COUNT(*) as count,
           ROUND(AVG(balance), 2) as avg_balance
    FROM primeinsurance.gold.dim_customer
    GROUP BY region ORDER BY count DESC
""").collect()
customer_kpis["by_region"] = [r.asDict() for r in cust_region]
risk = spark.sql("""
    SELECT 
        SUM(default_flag) as customers_with_default,
        SUM(hh_insurance) as customers_with_hh_insurance,
        SUM(car_loan) as customers_with_car_loan,
        ROUND(AVG(balance), 2) as avg_balance
    FROM primeinsurance.gold.dim_customer
""").collect()[0].asDict()
customer_kpis["risk_indicators"] = risk
education = spark.sql("""
    SELECT education, COUNT(*) as count
    FROM primeinsurance.gold.dim_customer
    WHERE education IS NOT NULL
    GROUP BY education ORDER BY count DESC
""").collect()
customer_kpis["by_education"] = [r.asDict() for r in education]
marital = spark.sql("""
    SELECT marital, COUNT(*) as count
    FROM primeinsurance.gold.dim_customer
    WHERE marital IS NOT NULL
    GROUP BY marital ORDER BY count DESC
""").collect()
customer_kpis["by_marital"] = [r.asDict() for r in marital]

print("\n=== DOMAIN 3: Customer Profile KPIs ===")
print(json.dumps(customer_kpis, indent=2, default=str))
all_kpis = {
    "policy_portfolio": policy_kpis,
    "claims_performance": claims_kpis,
    "customer_profile": customer_kpis
}

# COMMAND ----------


from openai import OpenAI
from pyspark.sql import Row
from datetime import datetime
import json, time

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(api_key=DATABRICKS_TOKEN, base_url=f"https://{WORKSPACE_URL}/serving-endpoints")
MODEL_NAME = "databricks-gpt-oss-20b"

def extract_text(raw):
    if isinstance(raw, list):
        for block in raw:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", str(raw))
        return str(raw)
    return str(raw) if not isinstance(raw, str) else raw
domains = [
    {
        "domain": "policy_portfolio",
        "title": "Policy Portfolio Executive Summary",
        "kpi_data": all_kpis["policy_portfolio"],
        "prompt_context": """You are the Chief Analytics Officer at PrimeInsurance writing a weekly executive brief for the CEO.
Using ONLY the KPI data below, write an executive summary of the policy portfolio.

Your summary must:
- Open with the headline number (total policies, total premium revenue)
- Highlight the distribution across coverage tiers and what it means for risk exposure
- Call out the umbrella coverage gap (how many policies lack umbrella protection)
- Flag any state concentration risk
- End with 2-3 specific, actionable recommendations

Write in executive tone — concise, data-driven, forward-looking. No jargon. Every claim must cite a specific number from the KPIs."""
    },
    {
        "domain": "claims_performance",
        "title": "Claims Performance Executive Summary",
        "kpi_data": all_kpis["claims_performance"],
        "prompt_context": """You are the Chief Analytics Officer at PrimeInsurance writing a weekly executive brief for the CEO.
Using ONLY the KPI data below, write an executive summary of claims performance.

Your summary must:
- Open with total claims volume and total financial exposure
- Analyze the rejection rate and what it signals about claim quality or fraud
- Compare performance across regions — which region is problematic and why
- Compare performance across coverage tiers — is the High tier generating disproportionate risk
- End with 2-3 specific, actionable recommendations

Write in executive tone — concise, data-driven, forward-looking. Every claim must cite a specific number from the KPIs."""
    },
    {
        "domain": "customer_profile",
        "title": "Customer Profile Executive Summary",
        "kpi_data": all_kpis["customer_profile"],
        "prompt_context": """You are the Chief Analytics Officer at PrimeInsurance writing a weekly executive brief for the CEO.
Using ONLY the KPI data below, write an executive summary of the customer base.

Your summary must:
- Open with the total verified customer count (after deduplication from 7 regional systems)
- Analyze regional distribution and balance patterns
- Highlight risk indicators: how many customers have defaults, lack household insurance, or carry car loans
- Assess the education and marital demographics and what they mean for product targeting
- End with 2-3 specific, actionable recommendations

Write in executive tone — concise, data-driven, forward-looking. Every claim must cite a specific number from the KPIs."""
    }
]

results = []
for domain in domains:
    full_prompt = f"""{domain['prompt_context']}

KPI DATA:
{json.dumps(domain['kpi_data'], indent=2, default=str)}

Write the executive summary now. Keep it to 200-300 words."""

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": full_prompt}],
            max_tokens=600,
            temperature=0.3
        )
        summary = extract_text(response.choices[0].message.content)
        status = "success"
    except Exception as e:
        summary = f"ERROR: {str(e)}"
        status = "failed"
    
    results.append(Row(
        domain_name=domain["domain"],
        summary_title=domain["title"],
        ai_executive_summary=summary,
        kpi_data_json=json.dumps(domain["kpi_data"], default=str),
        model_name=MODEL_NAME,
        generation_status=status,
        generated_at=datetime.now()
    ))
    
    print(f"{domain['domain']} — {status}")
    time.sleep(0.5)
df_results = spark.createDataFrame(results)
df_results.write.mode("overwrite").saveAsTable("primeinsurance.gold.ai_business_insights")
print(f"\nWritten to primeinsurance.gold.ai_business_insights")
print(f"Row count: {spark.table('primeinsurance.gold.ai_business_insights').count()}")

# COMMAND ----------


print("=== ai_query() Example 1: Claims Rejection by Region ===\n")

spark.sql("""
    SELECT ai_query(
        'databricks-gpt-oss-20b',
        'PrimeInsurance has 1000 claims with 27.6% rejection rate. West region has the highest rejection at 31.7% with 319 claims and $3.67M exposure. East region has lowest at 23.5% with 383 claims and $5.28M exposure. High-tier policies have 31% rejection vs Low-tier at 25.9%. Total financial exposure is $12.65M. Summarize this in 2 sentences for a CEO.'
    ) AS ai_summary
""").show(truncate=False)

print("=== ai_query() Example 2: Policy Portfolio Summary ===\n")

spark.sql("""
    SELECT ai_query(
        'databricks-gpt-oss-20b',
        'PrimeInsurance has 999 policies across 9 states generating $1.26M in annual premium revenue. Ohio leads with 186 policies. Coverage split: Low tier 348 policies, Medium 351, High 300. Only 201 of 999 policies (20%) have umbrella coverage averaging $5.5M. 80% of policyholders lack umbrella protection. Write a 2-sentence executive insight with one recommendation.'
    ) AS ai_summary
""").show(truncate=False)

# COMMAND ----------

df = spark.table("primeinsurance.gold.ai_business_insights")
print(f"=== Total rows: {df.count()} ===\n")

rows = df.select("domain_name", "summary_title", "ai_executive_summary", "generation_status").collect()

for row in rows:
    print(f"{'='*60}")
    print(f"Domain: {row.domain_name}")
    print(f"Title: {row.summary_title}")
    print(f"Status: {row.generation_status}")
    print(f"\nExecutive Summary:\n{row.ai_executive_summary}")
    print(f"{'='*60}\n")

# COMMAND ----------

