# Databricks notebook source
# MAGIC %sql 
# MAGIC select * from primeinsurance.silver.claims

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("primeinsurance.silver.claims")

print("=== Claims Silver Schema ===")
print(df.columns)
print(f"\nTotal claims: {df.count()}")

print("\n=== Financial stats (for z-score thresholds) ===")
df.select(
    F.mean("injury_amount").alias("avg_injury"),
    F.stddev("injury_amount").alias("std_injury"),
    F.mean("property_amount").alias("avg_property"),
    F.stddev("property_amount").alias("std_property"),
    F.mean("vehicle_amount").alias("avg_vehicle"),
    F.stddev("vehicle_amount").alias("std_vehicle"),
).show(truncate=False)

print("\n=== Total claim amount distribution ===")
df.withColumn("total_amount",
    F.coalesce(F.col("injury_amount"), F.lit(0)) +
    F.coalesce(F.col("property_amount"), F.lit(0)) +
    F.coalesce(F.col("vehicle_amount"), F.lit(0))
).select(
    F.mean("total_amount").alias("avg_total"),
    F.stddev("total_amount").alias("std_total"),
    F.max("total_amount").alias("max_total"),
    F.min("total_amount").alias("min_total"),
    F.expr("percentile_approx(total_amount, 0.95)").alias("p95_total")
).show(truncate=False)

print("\n=== incident_severity distribution ===")
df.groupBy("incident_severity").count().show()

print("\n=== claim_rejected distribution ===")
df.groupBy("claim_rejected").count().show()

print("\n=== bodily_injuries distribution ===")
df.groupBy("bodily_injuries").count().orderBy("bodily_injuries").show()

print("\n=== witnesses distribution ===")
df.groupBy("witnesses").count().orderBy("witnesses").show()

print("\n=== property_damage + police_report combinations ===")
df.groupBy("property_damage", "police_report_available").count().orderBy("count", ascending=False).show()

print("\n=== number_of_vehicles_involved distribution ===")
df.groupBy("number_of_vehicles_involved").count().orderBy("number_of_vehicles_involved").show()

print("\n=== authorities_contacted distribution ===")
df.groupBy("authorities_contacted").count().orderBy("count", ascending=False).show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
df = spark.table("primeinsurance.silver.claims")
df = df.withColumn("total_claim_amount",
    F.coalesce(F.col("injury_amount"), F.lit(0)) +
    F.coalesce(F.col("property_amount"), F.lit(0)) +
    F.coalesce(F.col("vehicle_amount"), F.lit(0))
)
AVG_TOTAL = 12652.40
STD_TOTAL = 10743.84

df = df.withColumn("amount_zscore",
    (F.col("total_claim_amount") - F.lit(AVG_TOTAL)) / F.lit(STD_TOTAL)
)
df = df.withColumn("rule1_high_amount",
    F.when(F.col("amount_zscore") > 2.0, 30)
     .when(F.col("amount_zscore") > 1.5, 20)
     .when(F.col("amount_zscore") > 1.0, 10)
     .otherwise(0)
)
df = df.withColumn("rule2_severity_mismatch",
    F.when(
        (F.col("incident_severity") == "Trivial Damage") &
        (F.col("total_claim_amount") > AVG_TOTAL), 25
    ).when(
        (F.col("incident_severity") == "Minor Damage") &
        (F.col("total_claim_amount") > 35665), 20  # above p95
    ).otherwise(0)
)

df = df.withColumn("rule3_missing_documentation",
    F.when(
        (F.col("property_damage") == "YES") &
        (F.col("police_report_available").isNull()), 20
    ).when(
        (F.col("property_damage") == "YES") &
        (F.col("police_report_available") == "NO"), 15
    ).when(
        F.col("authorities_contacted") == "None", 10
    ).otherwise(0)
)

df = df.withColumn("rule4_no_witnesses_multi_vehicle",
    F.when(
        (F.col("number_of_vehicles_involved") >= 3) &
        (F.col("witnesses") == 0), 15
    ).when(
        (F.col("number_of_vehicles_involved") >= 3) &
        (F.col("witnesses") == 1), 8
    ).otherwise(0)
)

df = df.withColumn("rule5_injury_without_property",
    F.when(
        (F.col("bodily_injuries") == 2) &
        ((F.col("property_damage") == "NO") | F.col("property_damage").isNull()), 10
    ).when(
        (F.col("bodily_injuries") >= 1) &
        (F.col("property_damage") == "NO") &
        (F.col("vehicle_amount").isNull()), 10
    ).otherwise(0)
)

df = df.withColumn("anomaly_score",
    F.col("rule1_high_amount") +
    F.col("rule2_severity_mismatch") +
    F.col("rule3_missing_documentation") +
    F.col("rule4_no_witnesses_multi_vehicle") +
    F.col("rule5_injury_without_property")
)
df = df.withColumn("anomaly_score",
    F.when(F.col("anomaly_score") > 100, 100).otherwise(F.col("anomaly_score"))
)
df = df.withColumn("priority_tier",
    F.when(F.col("anomaly_score") >= 40, "HIGH")
     .when(F.col("anomaly_score") >= 20, "MEDIUM")
     .otherwise("LOW")
)

df = df.withColumn("triggered_rules",
    F.concat_ws(", ",
        F.when(F.col("rule1_high_amount") > 0, F.lit("HIGH_AMOUNT")),
        F.when(F.col("rule2_severity_mismatch") > 0, F.lit("SEVERITY_MISMATCH")),
        F.when(F.col("rule3_missing_documentation") > 0, F.lit("MISSING_DOCUMENTATION")),
        F.when(F.col("rule4_no_witnesses_multi_vehicle") > 0, F.lit("NO_WITNESSES_MULTI_VEHICLE")),
        F.when(F.col("rule5_injury_without_property") > 0, F.lit("INJURY_WITHOUT_PROPERTY"))
    )
)
print("=== Anomaly Score Distribution ===")
df.groupBy("priority_tier").count().orderBy("priority_tier").show()

print("=== Score stats ===")
df.select(
    F.mean("anomaly_score").alias("avg_score"),
    F.max("anomaly_score").alias("max_score"),
    F.min("anomaly_score").alias("min_score")
).show()

print("=== HIGH priority claims (sample) ===")
df.filter(F.col("priority_tier") == "HIGH") \
  .select("claim_id", "anomaly_score", "priority_tier",
          "total_claim_amount", "incident_severity", "triggered_rules") \
  .orderBy(F.col("anomaly_score").desc()) \
  .limit(5).show(truncate=False)
df.createOrReplaceTempView("scored_claims")
high_count = df.filter(F.col("priority_tier") == "HIGH").count()
medium_count = df.filter(F.col("priority_tier") == "MEDIUM").count()
print(f"\nFlagged for investigation: {high_count} HIGH + {medium_count} MEDIUM = {high_count + medium_count} total")
print(f"Out of 1000 claims = {(high_count + medium_count)/10:.1f}% flagged")

# COMMAND ----------

# DBTITLE 1,Cell 4
from openai import OpenAI
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
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
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for block in parsed:
                    if isinstance(block, dict) and block.get("type") == "text":
                        return block.get("text", raw)
        except:
            pass
        return raw
    return str(raw)

def build_anomaly_prompt(claim):
    return f"""You are a senior claims fraud investigator at PrimeInsurance.
A claim has been statistically flagged by our anomaly detection system.
Write an investigation brief for a claims adjuster.

FLAGGED CLAIM DETAILS:
- Claim ID: {claim['claim_id']}
- Policy ID: {claim['policy_id']}
- Total Claim Amount: ${claim['total_claim_amount']:,.2f}
- Injury Amount: ${claim['injury_amount'] or 0:,.2f}
- Property Amount: ${claim['property_amount'] or 0:,.2f}
- Vehicle Amount: ${claim['vehicle_amount'] or 0:,.2f}
- Incident Severity: {claim['incident_severity']}
- Incident Type: {claim['incident_type']}
- Collision Type: {claim['collision_type']}
- Vehicles Involved: {claim['number_of_vehicles_involved']}
- Bodily Injuries: {claim['bodily_injuries']}
- Witnesses: {claim['witnesses']}
- Property Damage Reported: {claim['property_damage']}
- Police Report Available: {claim['police_report_available']}
- Authorities Contacted: {claim['authorities_contacted']}
- Anomaly Score: {claim['anomaly_score']}/100 ({claim['priority_tier']})
- Triggered Rules: {claim['triggered_rules']}

FORMAT YOUR RESPONSE EXACTLY AS:

SUSPICION SUMMARY: [One sentence citing actual dollar amounts and data points]

RISK FACTORS:
1. [First risk factor with specific data from this claim]
2. [Second risk factor with specific data]
3. [Third risk factor if applicable]

RECOMMENDED ACTIONS:
1. [First specific action the investigator should take]
2. [Second specific action]
3. [Third action if needed]

Be specific to THIS claim. Use dollar amounts and data points."""

AVG_TOTAL = 12652.40
STD_TOTAL = 10743.84

df_all = spark.table("primeinsurance.silver.claims")
df_all = df_all.withColumn("total_claim_amount",
    F.coalesce(F.col("injury_amount"), F.lit(0)) +
    F.coalesce(F.col("property_amount"), F.lit(0)) +
    F.coalesce(F.col("vehicle_amount"), F.lit(0))
).withColumn("amount_zscore",
    (F.col("total_claim_amount") - F.lit(AVG_TOTAL)) / F.lit(STD_TOTAL)
).withColumn("rule1_high_amount",
    F.when(F.col("amount_zscore") > 2.0, 30).when(F.col("amount_zscore") > 1.5, 20).when(F.col("amount_zscore") > 1.0, 10).otherwise(0)
).withColumn("rule2_severity_mismatch",
    F.when((F.col("incident_severity") == "Trivial Damage") & (F.col("total_claim_amount") > AVG_TOTAL), 25)
    .when((F.col("incident_severity") == "Minor Damage") & (F.col("total_claim_amount") > 35665), 20).otherwise(0)
).withColumn("rule3_missing_documentation",
    F.when((F.col("property_damage") == "YES") & (F.col("police_report_available").isNull()), 20)
    .when((F.col("property_damage") == "YES") & (F.col("police_report_available") == "NO"), 15)
    .when(F.col("authorities_contacted") == "None", 10).otherwise(0)
).withColumn("rule4_no_witnesses_multi_vehicle",
    F.when((F.col("number_of_vehicles_involved") >= 3) & (F.col("witnesses") == 0), 15)
    .when((F.col("number_of_vehicles_involved") >= 3) & (F.col("witnesses") == 1), 8).otherwise(0)
).withColumn("rule5_injury_without_property",
    F.when((F.col("bodily_injuries") == 2) & ((F.col("property_damage") == "NO") | F.col("property_damage").isNull()), 10)
    .when((F.col("bodily_injuries") >= 1) & (F.col("property_damage") == "NO") & (F.col("vehicle_amount").isNull()), 10).otherwise(0)
).withColumn("anomaly_score",
    F.least(F.lit(100), F.col("rule1_high_amount") + F.col("rule2_severity_mismatch") + F.col("rule3_missing_documentation") + F.col("rule4_no_witnesses_multi_vehicle") + F.col("rule5_injury_without_property"))
).withColumn("priority_tier",
    F.when(F.col("anomaly_score") >= 40, "HIGH").when(F.col("anomaly_score") >= 20, "MEDIUM").otherwise("LOW")
).withColumn("triggered_rules",
    F.concat_ws(", ",
        F.when(F.col("rule1_high_amount") > 0, F.lit("HIGH_AMOUNT")),
        F.when(F.col("rule2_severity_mismatch") > 0, F.lit("SEVERITY_MISMATCH")),
        F.when(F.col("rule3_missing_documentation") > 0, F.lit("MISSING_DOCUMENTATION")),
        F.when(F.col("rule4_no_witnesses_multi_vehicle") > 0, F.lit("NO_WITNESSES_MULTI_VEHICLE")),
        F.when(F.col("rule5_injury_without_property") > 0, F.lit("INJURY_WITHOUT_PROPERTY")))
)

all_claims = df_all.orderBy(F.col("anomaly_score").desc()).collect()
flagged = [c for c in all_claims if c.asDict()["priority_tier"] in ("HIGH", "MEDIUM")]
low = [c for c in all_claims if c.asDict()["priority_tier"] == "LOW"]

print(f"Scored 1000 claims: {len(flagged)} flagged (HIGH+MEDIUM), {len(low)} LOW")
print("=" * 60)
results = []
for i, claim in enumerate(flagged):
    cd = claim.asDict()
    prompt = build_anomaly_prompt(cd)
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3
        )
        brief = extract_text(response.choices[0].message.content)
        status = "success"
    except Exception as e:
        brief = f"ERROR: {str(e)}"
        status = "failed"
    
    results.append(Row(
        claim_id=cd["claim_id"], policy_id=cd["policy_id"],
        incident_severity=cd["incident_severity"], incident_type=cd["incident_type"],
        collision_type=cd["collision_type"],
        total_claim_amount=float(cd["total_claim_amount"]),
        injury_amount=float(cd["injury_amount"] or 0),
        property_amount=float(cd["property_amount"] or 0),
        vehicle_amount=float(cd["vehicle_amount"] or 0),
        number_of_vehicles_involved=int(cd["number_of_vehicles_involved"]),
        bodily_injuries=int(cd["bodily_injuries"]), witnesses=int(cd["witnesses"]),
        property_damage=cd["property_damage"],
        police_report_available=cd["police_report_available"],
        authorities_contacted=cd["authorities_contacted"],
        claim_rejected=cd["claim_rejected"],
        anomaly_score=int(cd["anomaly_score"]), priority_tier=cd["priority_tier"],
        triggered_rules=cd["triggered_rules"],
        ai_investigation_brief=brief, model_name=MODEL_NAME,
        generation_status=status, generated_at=datetime.now()
    ))
    
    if (i+1) % 25 == 0 or (i+1) == len(flagged):
        print(f"[{i+1}/{len(flagged)}] LLM briefs generated")
    time.sleep(0.3)
for claim in low:
    cd = claim.asDict()
    results.append(Row(
        claim_id=cd["claim_id"], policy_id=cd["policy_id"],
        incident_severity=cd["incident_severity"], incident_type=cd["incident_type"],
        collision_type=cd["collision_type"],
        total_claim_amount=float(cd["total_claim_amount"]),
        injury_amount=float(cd["injury_amount"] or 0),
        property_amount=float(cd["property_amount"] or 0),
        vehicle_amount=float(cd["vehicle_amount"] or 0),
        number_of_vehicles_involved=int(cd["number_of_vehicles_involved"]),
        bodily_injuries=int(cd["bodily_injuries"]), witnesses=int(cd["witnesses"]),
        property_damage=cd["property_damage"],
        police_report_available=cd["police_report_available"],
        authorities_contacted=cd["authorities_contacted"],
        claim_rejected=cd["claim_rejected"],
        anomaly_score=int(cd["anomaly_score"]), priority_tier=cd["priority_tier"],
        triggered_rules=cd["triggered_rules"],
        ai_investigation_brief=None, model_name=None,
        generation_status="not_flagged", generated_at=datetime.now()
    ))
print(f"\n{'='*60}")
success_count = sum(1 for r in results if r.generation_status == 'success')
failed_count = sum(1 for r in results if r.generation_status == 'failed')
print(f"LLM briefs: {success_count} success, {failed_count} failed")
print(f"Total rows: {len(results)}")

df_results = spark.createDataFrame(results)
df_results.write.mode("overwrite").saveAsTable("primeinsurance.gold.claim_anomaly_explanations")
print(f"\nWritten to primeinsurance.gold.claim_anomaly_explanations")
print(f"Row count: {spark.table('primeinsurance.gold.claim_anomaly_explanations').count()}")

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.table("primeinsurance.gold.claim_anomaly_explanations")
print("=== Total rows (should be 1000) ===")
print(df.count())
print("\n=== Priority distribution ===")
df.groupBy("priority_tier").count().orderBy("priority_tier").show()
print("\n=== Generation status ===")
df.groupBy("generation_status").count().show()
print("\n=== TOP 3 HIGH-priority investigation briefs ===")
rows = df.filter("priority_tier = 'HIGH'") \
    .orderBy(F.col("anomaly_score").desc()) \
    .select("claim_id", "anomaly_score", "total_claim_amount",
            "incident_severity", "triggered_rules", "ai_investigation_brief") \
    .limit(3).collect()
for row in rows:
    print(f"\n{'='*60}")
    print(f"Claim: {row.claim_id} | Score: {row.anomaly_score} | Amount: ${row.total_claim_amount:,.2f}")
    print(f"Severity: {row.incident_severity}")
    print(f"Rules: {row.triggered_rules}")
    print(f"\nInvestigation Brief:\n{row.ai_investigation_brief}")
    print(f"{'='*60}")
print("\n=== Sample MEDIUM-priority brief ===")
med_rows = df.filter("priority_tier = 'MEDIUM'") \
    .orderBy(F.col("anomaly_score").desc()) \
    .select("claim_id", "anomaly_score", "triggered_rules", "ai_investigation_brief") \
    .limit(1).collect()
for row in med_rows:
    print(f"\nClaim: {row.claim_id} | Score: {row.anomaly_score}")
    print(f"Rules: {row.triggered_rules}")
    print(f"\nBrief:\n{row.ai_investigation_brief}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from primeinsurance.gold.claim_anomaly_explanations

# COMMAND ----------

