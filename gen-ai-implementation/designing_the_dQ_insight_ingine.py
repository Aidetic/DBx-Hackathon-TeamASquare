# Databricks notebook source
from openai import OpenAI
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
response = client.chat.completions.create(
    model="databricks-gpt-oss-20b",
    messages=[{"role": "user", "content": "Say hello in one sentence."}],
    max_tokens=100
)
raw = response.choices[0].message.content
print("=== RAW RESPONSE ===")
print(raw)
print("\n=== TYPE ===")
print(type(raw))
import json
try:
    parsed = json.loads(raw)
    print("\n=== PARSED JSON ===")
    for block in parsed:
        print(f"Type: {block.get('type')}, Content: {str(block)[:200]}")
        if block.get("type") == "text":
            print(f"\n=== EXTRACTED TEXT ===\n{block['text']}")
except Exception as e:
    print(f"\n=== Not JSON format: {e} ===")
    print("Response is plain text")

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
dq_issues = [
    Row(issue_id="DQ-001", table_name="customers", column_name="CustomerID",
        rule_name="valid_customer_id", issue_type="schema_variation",
        severity="critical", affected_records=3605, total_records=3605,
        affected_ratio=1.0,
        description="Customer ID stored under 3 different column names: CustomerID, Customer_ID, cust_id across 7 regional files",
        suggested_fix="COALESCE(CustomerID, Customer_ID, cust_id) to unify into single customer_id column",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-002", table_name="customers", column_name="Region",
        rule_name="valid_region", issue_type="schema_variation",
        severity="high", affected_records=199, total_records=3605,
        affected_ratio=0.055,
        description="customers_5.csv uses abbreviated region codes E, W, C, S instead of full names East, West, Central, South",
        suggested_fix="Map single-letter abbreviations to full region names: E→East, W→West, C→Central, S→South",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-003", table_name="customers", column_name="Region",
        rule_name="valid_region", issue_type="schema_variation",
        severity="high", affected_records=199, total_records=3605,
        affected_ratio=0.055,
        description="customers_1.csv stores Region in column named Reg instead of Region",
        suggested_fix="COALESCE(Region, Reg) to unify column name",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-004", table_name="customers", column_name="Education",
        rule_name="valid_education", issue_type="column_swap",
        severity="critical", affected_records=199, total_records=3605,
        affected_ratio=0.055,
        description="customers_6.csv has Education and Marital_status columns swapped — Education contains marital values (single/married/divorced), Marital_status contains education values (primary/secondary/tertiary)",
        suggested_fix="Detect swap by checking if Education contains marital-like values, then reverse the assignment",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-005", table_name="customers", column_name="Education",
        rule_name="valid_education", issue_type="data_corruption",
        severity="medium", affected_records=73, total_records=3605,
        affected_ratio=0.02,
        description="customers_5.csv contains truncated value 'terto' instead of 'tertiary' in Education column — 73 records affected",
        suggested_fix="Replace 'terto' with 'tertiary' during Silver transformation",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-006", table_name="customers", column_name="Marital_status",
        rule_name="valid_marital", issue_type="missing_column",
        severity="high", affected_records=200, total_records=3605,
        affected_ratio=0.055,
        description="customers_2.csv has no Marital or Marital_status column at all — 200 records have NULL marital status",
        suggested_fix="Accept NULL as valid for marital since source system never captured this field",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-007", table_name="customers", column_name="HHInsurance",
        rule_name="missing_column", issue_type="missing_column",
        severity="medium", affected_records=200, total_records=3605,
        affected_ratio=0.055,
        description="customers_2.csv is missing the HHInsurance column entirely — 200 records have NULL",
        suggested_fix="Set HHInsurance to NULL for customers_2 records; flag for source system remediation",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-008", table_name="claims", column_name="incident_date",
        rule_name="valid_date_format", issue_type="data_corruption",
        severity="critical", affected_records=1000, total_records=1000,
        affected_ratio=1.0,
        description="All 1000 claims have corrupted incident_date values like '27:00.0', '45:00.0' — these are minute fragments from broken datetime serialization, not recoverable dates",
        suggested_fix="Set incident_date to NULL in Silver; flag as unrecoverable; document for source system fix",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-009", table_name="claims", column_name="Claim_Processed_On",
        rule_name="null_string_check", issue_type="data_corruption",
        severity="high", affected_records=526, total_records=1000,
        affected_ratio=0.526,
        description="526 claims have string 'NULL' in Claim_Processed_On instead of actual NULL — these are unprocessed claims stored as string literal",
        suggested_fix="Replace string 'NULL' with actual NULL during Silver transformation",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-010", table_name="claims", column_name="property_damage",
        rule_name="unknown_value_check", issue_type="unknown_values",
        severity="medium", affected_records=360, total_records=1000,
        affected_ratio=0.36,
        description="360 claims have '?' as property_damage value instead of YES/NO/NULL — data was never captured",
        suggested_fix="Replace '?' with NULL in Silver; same applies to police_report_available (343 records)",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-011", table_name="claims", column_name="vehicle",
        rule_name="type_cast_check", issue_type="type_mismatch",
        severity="medium", affected_records=29, total_records=1000,
        affected_ratio=0.029,
        description="29 claims have string 'NULL' in vehicle amount column — prevents casting to float for financial aggregation",
        suggested_fix="Replace string 'NULL' with actual NULL before casting to FloatType; accept NULL vehicle_amount as valid",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-012", table_name="sales", column_name="sales_id",
        rule_name="blank_row_check", issue_type="blank_rows",
        severity="critical", affected_records=3132, total_records=4981,
        affected_ratio=0.629,
        description="3132 out of 4981 sales records are completely blank — every column is NULL including sales_id, car_id, ad_placed_on. These are CSV padding artifacts, not real data",
        suggested_fix="Filter out rows where sales_id IS NULL before any Silver processing — do not quarantine, just exclude",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-013", table_name="policy", column_name="umbrella_limit",
        rule_name="valid_umbrella_limit", issue_type="invalid_value",
        severity="high", affected_records=1, total_records=1000,
        affected_ratio=0.001,
        description="1 policy record has negative umbrella_limit value of -1000000 — physically impossible for an insurance limit",
        suggested_fix="Route to quarantine (silver.policy_failed) with rejection reason 'negative umbrella_limit'",
        detected_at=datetime(2026, 3, 24)),
    
    Row(issue_id="DQ-014", table_name="customers", column_name="customer_id",
        rule_name="duplicate_check", issue_type="duplicate_records",
        severity="critical", affected_records=2001, total_records=3605,
        affected_ratio=0.555,
        description="customers_7.csv is the master file containing all 1604 unique customers. All 6 regional files are subsets. 2001 records are duplicates that appear in both regional files and the master",
        suggested_fix="Deduplicate using ROW_NUMBER partitioned by customer_id, prioritizing customers_7 as source of truth",
        detected_at=datetime(2026, 3, 24)),
]
df_dq = spark.createDataFrame(dq_issues)

df_dq.write.mode("overwrite").saveAsTable("primeinsurance.silver.dq_issues")

print("=== dq_issues table created ===")
print(f"Row count: {spark.table('primeinsurance.silver.dq_issues').count()}")
spark.table("primeinsurance.silver.dq_issues").select(
    "issue_id", "table_name", "column_name", "severity", "affected_records"
).show(truncate=False)

# COMMAND ----------

from openai import OpenAI
from pyspark.sql import Row
from datetime import datetime
import json, time
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
MODEL_NAME = "databricks-gpt-oss-20b"
def extract_text_from_response(raw_content):
    """
    databricks-gpt-oss-20b returns a Python list (already parsed):
    [{'type': 'reasoning', 'summary': [...]}, {'type': 'text', 'text': 'actual answer'}]
    
    The response is already a list object — NOT a JSON string.
    So we iterate directly, no json.loads() needed.
    """
    if isinstance(raw_content, list):
        for block in raw_content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", str(raw_content))
        return str(raw_content)
    if isinstance(raw_content, str):
        try:
            parsed = json.loads(raw_content)
            if isinstance(parsed, list):
                for block in parsed:
                    if isinstance(block, dict) and block.get("type") == "text":
                        return block.get("text", raw_content)
            return raw_content
        except (json.JSONDecodeError, TypeError):
            return raw_content
    return str(raw_content)
def build_dq_prompt(issue):
    return f"""You are a Data Quality Analyst at PrimeInsurance, an auto insurance company.
A data quality issue was detected in the data pipeline. Translate this technical finding 
into a clear, structured explanation that a compliance officer (non-technical) can understand and act on.

TECHNICAL DETAILS:
- Issue ID: {issue['issue_id']}
- Table: {issue['table_name']}
- Column: {issue['column_name']}
- Rule Name: {issue['rule_name']}
- Issue Type: {issue['issue_type']}
- Severity: {issue['severity']}
- Affected Records: {issue['affected_records']} out of {issue['total_records']} ({issue['affected_ratio']*100:.1f}%)
- Technical Description: {issue['description']}
- Suggested Fix: {issue['suggested_fix']}

FORMAT YOUR RESPONSE EXACTLY AS:

FINDING: [One sentence plain English summary of what was found]

BUSINESS IMPACT: [Why this matters to PrimeInsurance's operations, compliance, or revenue]

ROOT CAUSE: [What caused this issue in the source systems]

REMEDIATION APPLIED: [What the data pipeline did to fix this]

PREVENTION RECOMMENDATION: [What should be done to prevent this from recurring]

Keep each section to 1-2 sentences. Use business language, not technical jargon."""
df_issues = spark.table("primeinsurance.silver.dq_issues")
issues = df_issues.collect()

print(f"Processing {len(issues)} DQ issues through LLM...")
print("=" * 60)
results = []
for i, issue in enumerate(issues):
    issue_dict = issue.asDict()
    prompt = build_dq_prompt(issue_dict)
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3
        )
        
        raw = response.choices[0].message.content
        explanation = extract_text_from_response(raw)
        status = "success"
        
    except Exception as e:
        explanation = f"ERROR: {str(e)}"
        status = "failed"
    
    results.append(Row(
        issue_id=issue_dict["issue_id"],
        table_name=issue_dict["table_name"],
        column_name=issue_dict["column_name"],
        rule_name=issue_dict["rule_name"],
        issue_type=issue_dict["issue_type"],
        severity=issue_dict["severity"],
        affected_records=issue_dict["affected_records"],
        total_records=issue_dict["total_records"],
        affected_ratio=float(issue_dict["affected_ratio"]),
        ai_explanation=explanation,
        model_name=MODEL_NAME,
        generation_status=status,
        generated_at=datetime.now()
    ))
    
    print(f"[{i+1}/{len(issues)}] {issue_dict['issue_id']} ({issue_dict['table_name']}.{issue_dict['column_name']}) — {status}")
    time.sleep(0.5)

print("\n" + "=" * 60)
print(f"Completed: {sum(1 for r in results if r.generation_status == 'success')} success, "
      f"{sum(1 for r in results if r.generation_status == 'failed')} failed")
df_results = spark.createDataFrame(results)
df_results.write.mode("overwrite").saveAsTable("primeinsurance.gold.dq_explanation_report")

print(f"\nWritten to primeinsurance.gold.dq_explanation_report")
print(f"Row count: {spark.table('primeinsurance.gold.dq_explanation_report').count()}")

# COMMAND ----------

print("=== DQ Explanation Report — Row Count ===")
df_report = spark.table("primeinsurance.gold.dq_explanation_report")
print(f"Total rows: {df_report.count()}")
print("\n=== Generation Status ===")
df_report.groupBy("generation_status").count().show()
print("\n=== Sample Output: 3 rows with AI explanations ===")
rows = df_report.select(
    "issue_id", "table_name", "column_name", "severity",
    "affected_records", "ai_explanation", "generation_status"
).limit(3).collect()
for row in rows:
    print(f"\n{'='*60}")
    print(f"Issue: {row.issue_id} | Table: {row.table_name} | Column: {row.column_name}")
    print(f"Severity: {row.severity} | Affected: {row.affected_records} | Status: {row.generation_status}")
    print(f"\nAI Explanation:\n{row.ai_explanation}")
    print(f"{'='*60}")