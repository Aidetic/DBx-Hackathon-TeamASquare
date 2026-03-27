import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

SILVER = "primeinsurance.silver"


@dlt.table(
    name="dim_customer",
    comment="Customer master — 1,604 deduplicated records unified from 7 regional sources",
    table_properties={"quality": "gold"}
)
def gold_dim_customer():
    return spark.read.table(f"{SILVER}.customers").select(
        "customer_id", "region", "state", "city", "job",
        "marital", "education", "default_flag", "balance",
        "hh_insurance", "car_loan", "_source_file", "_load_timestamp"
    )


@dlt.table(
    name="dim_policy",
    comment="Policy dimension — 999 clean records with coverage type and premium details",
    table_properties={"quality": "gold"}
)
def gold_dim_policy():
    return (
        spark.read.table(f"{SILVER}.policy")
        .withColumn("policy_csl_tier",
            F.when(F.col("policy_csl") == "100/300",  "Low")
             .when(F.col("policy_csl") == "250/500",  "Medium")
             .when(F.col("policy_csl") == "500/1000", "High")
             .otherwise("Unknown")
        )
        .select(
            "policy_number", "policy_bind_date", "policy_state",
            "policy_csl", "policy_csl_tier", "policy_deductable",
            "policy_annual_premium", "umbrella_limit",
            "car_id", "customer_id", "_source_file", "_load_timestamp"
        )
    )


@dlt.table(
    name="dim_car",
    comment="Vehicle catalogue dimension — 2,500 records with numeric specs (unit suffixes stripped in Silver)",
    table_properties={"quality": "gold"}
)
def gold_dim_car():
    return spark.read.table(f"{SILVER}.cars").select(
        "car_id", "name", "model", "fuel", "transmission",
        "km_driven", "mileage_kmpl", "engine_cc",
        "max_power_bhp", "torque_raw", "seats",
        "_source_file", "_load_timestamp"
    )


@dlt.table(
    name="dim_date",
    comment="Calendar dimension generated from Silver date ranges. 4018 days (2010-2020).",
    table_properties={"quality": "gold"}
)
def gold_dim_date():
    # incident_date is corrupted at source so claims can't join on this — used for policy and sales only
    return (
        spark.range(0, 4018)
        .select(F.expr("date_add(date('2010-01-01'), cast(id as int))").alias("full_date"))
        .select(
            F.date_format("full_date", "yyyyMMdd").alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.when(F.dayofweek("full_date").isin(1, 7), 1).otherwise(0).alias("is_weekend")
        )
    )


@dlt.table(
    name="fact_claims",
    comment="Claims fact — 1,000 records. Grain: one claim. policy_csl and region denormalized for performance.",
    table_properties={"quality": "gold"}
)
def gold_fact_claims():
    claims = spark.read.table(f"{SILVER}.claims")

    policy = (
        spark.read.table(f"{SILVER}.policy")
        .withColumn("policy_csl_tier",
            F.when(F.col("policy_csl") == "100/300",  "Low")
             .when(F.col("policy_csl") == "250/500",  "Medium")
             .when(F.col("policy_csl") == "500/1000", "High")
             .otherwise("Unknown")
        )
        .select("policy_number", "policy_csl", "policy_csl_tier",
                "policy_deductable", "policy_annual_premium", "customer_id")
    )

    customers = spark.read.table(f"{SILVER}.customers").select("customer_id", "region", "state")

    return (
        claims
        .join(policy, claims.policy_id == policy.policy_number, "left")
        .join(customers, "customer_id", "left")
        .select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("customer_id"),
            # policy and customer fields denormalized to avoid repeated 3-table joins on dashboards
            F.col("policy_csl"),
            F.col("policy_csl_tier"),
            F.col("policy_deductable"),
            F.col("policy_annual_premium"),
            F.col("region"),
            F.col("state"),
            F.col("incident_severity"),
            F.col("incident_type"),
            F.col("collision_type"),
            F.col("incident_state"),
            F.col("incident_city"),
            F.col("authorities_contacted"),
            F.col("property_damage"),
            F.col("police_report_available"),
            F.col("claim_rejected"),
            F.when(F.col("claim_rejected") == "Y", 1).otherwise(0).alias("is_rejected"),
            F.col("injury_amount"),
            F.col("property_amount"),
            F.col("vehicle_amount"),
            # vehicle is NULL for 29 records where damage wasn't assessed — coalesce to 0 for the sum
            (
                F.coalesce(F.col("injury_amount"),   F.lit(0.0)) +
                F.coalesce(F.col("property_amount"), F.lit(0.0)) +
                F.coalesce(F.col("vehicle_amount"),  F.lit(0.0))
            ).alias("total_claim_amount"),
            F.col("number_of_vehicles_involved"),
            F.col("bodily_injuries"),
            F.col("witnesses"),
            # date fields corrupted at source — retained as raw strings for audit only
            F.col("incident_date_raw"),
            F.col("claim_logged_on_raw"),
            F.col("claim_processed_on_raw"),
            F.col("_source_file"),
            F.col("_load_timestamp")
        )
    )


@dlt.table(
    name="fact_sales",
    comment="Sales fact — 1,849 valid records. Grain: one listing. 164 unsold = core revenue leakage metric.",
    table_properties={"quality": "gold"}
)
def gold_fact_sales():
    sales = spark.read.table(f"{SILVER}.sales")
    cars = spark.read.table(f"{SILVER}.cars").select(
        "car_id", "name", "model", "fuel", "transmission", "seats"
    )

    return (
        sales
        .join(cars, "car_id", "left")
        .select(
            F.col("sales_id"),
            F.col("car_id"),
            # car fields denormalized so inventory queries don't need a join every time
            F.col("name").alias("car_name"),
            F.col("model").alias("car_model"),
            F.col("fuel"),
            F.col("transmission"),
            F.col("seats"),
            F.col("region"),
            F.col("state"),
            F.col("city"),
            F.col("seller_type"),
            F.col("owner"),
            F.col("ad_placed_on"),
            F.col("sold_on"),
            F.year("ad_placed_on").alias("listing_year"),
            F.month("ad_placed_on").alias("listing_month"),
            F.when(F.col("sold_on").isNotNull(), 1).otherwise(0).alias("is_sold"),
            F.col("original_selling_price"),
            F.col("days_unsold"),
            # revenue_at_risk is only populated for unsold records — NULL for sold cars
            F.when(F.col("sold_on").isNull(), F.col("original_selling_price"))
             .otherwise(F.lit(None).cast(FloatType()))
             .alias("revenue_at_risk"),
            F.col("_source_file"),
            F.col("_load_timestamp")
        )
    )


# PRE-AGGREGATED TABLES
# Refreshed on every pipeline run so dashboards query these
# instead of scanning the full fact tables each time.

@dlt.table(
    name="agg_claim_rejection_by_policy_region",
    comment="Rejection rate by policy_csl and region. Refreshes with every pipeline run. Morning standup metric.",
    table_properties={"quality": "gold"}
)
def gold_agg_rejection():
    return (
        spark.read.table("primeinsurance.gold.fact_claims")
        .groupBy("policy_csl", "policy_csl_tier", "region")
        .agg(
            F.count("*").alias("total_claims"),
            F.sum("is_rejected").alias("rejected_claims"),
            F.round(F.sum("is_rejected") * 100.0 / F.count("*"), 2).alias("rejection_rate_pct"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim_amount"),
            F.round(F.sum("total_claim_amount"), 2).alias("total_claim_amount")
        )
        .orderBy(F.col("rejection_rate_pct").desc())
    )


@dlt.table(
    name="agg_claim_severity_breakdown",
    comment="Claim counts and financial totals by incident severity. Used by claims adjusters.",
    table_properties={"quality": "gold"}
)
def gold_agg_severity():
    return (
        spark.read.table("primeinsurance.gold.fact_claims")
        .groupBy("incident_severity")
        .agg(
            F.count("*").alias("total_claims"),
            F.sum("is_rejected").alias("rejected_claims"),
            F.round(F.sum("is_rejected") * 100.0 / F.count("*"), 2).alias("rejection_rate_pct"),
            F.round(F.sum("total_claim_amount"), 2).alias("total_claim_amount"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim_amount"),
            F.round(F.avg("bodily_injuries"), 2).alias("avg_bodily_injuries"),
            F.round(F.avg("number_of_vehicles_involved"), 2).alias("avg_vehicles_involved")
        )
        .orderBy("total_claims", ascending=False)
    )


@dlt.table(
    name="agg_customer_count_by_region",
    comment="Deduplicated customer count by region. The regulatory submission number — always auditable.",
    table_properties={"quality": "gold"}
)
def gold_agg_customers():
    return (
        spark.read.table("primeinsurance.gold.dim_customer")
        .groupBy("region")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("state").alias("states_covered"),
            F.round(F.avg("balance"), 2).alias("avg_balance"),
            F.sum(F.when(F.col("default_flag") == 1, 1).otherwise(0)).alias("customers_with_default"),
            F.sum(F.when(F.col("hh_insurance") == 1, 1).otherwise(0)).alias("customers_with_hh_insurance"),
            F.sum(F.when(F.col("car_loan") == 1, 1).otherwise(0)).alias("customers_with_car_loan")
        )
        .orderBy("region")
    )


@dlt.table(
    name="agg_unsold_inventory_by_model_region",
    comment="Aging unsold inventory by car model and region. Revenue at risk. Core Revenue Leakage metric.",
    table_properties={"quality": "gold"}
)
def gold_agg_inventory():
    return (
        spark.read.table("primeinsurance.gold.fact_sales")
        .filter(F.col("is_sold") == 0)
        .groupBy("car_model", "region")
        .agg(
            F.count("*").alias("unsold_count"),
            F.max("days_unsold").alias("max_days_unsold"),
            F.round(F.avg("days_unsold"), 1).alias("avg_days_unsold"),
            F.min("days_unsold").alias("min_days_unsold"),
            F.round(F.sum("revenue_at_risk"), 2).alias("total_revenue_at_risk"),
            F.round(F.avg("original_selling_price"), 2).alias("avg_listing_price"),
            F.sum(F.when(F.col("days_unsold") > 60, 1).otherwise(0)).alias("unsold_over_60_days"),
            F.sum(F.when(F.col("days_unsold") > 90, 1).otherwise(0)).alias("unsold_over_90_days")
        )
        .orderBy(F.col("max_days_unsold").desc())
    )


@dlt.table(
    name="agg_policy_premium_by_coverage_tier",
    comment="Policy count, premium revenue, and claim exposure by coverage tier. Weekly leadership metric.",
    table_properties={"quality": "gold"}
)
def gold_agg_premium():
    policy = spark.read.table("primeinsurance.gold.dim_policy")

    claims_agg = (
        spark.read.table("primeinsurance.gold.fact_claims")
        .groupBy("policy_id")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum("is_rejected").alias("rejected_count"),
            F.sum("total_claim_amount").alias("total_claims_value")
        )
    )

    return (
        policy
        .join(claims_agg, policy.policy_number == claims_agg.policy_id, "left")
        .groupBy("policy_csl", "policy_csl_tier")
        .agg(
            F.count("*").alias("policy_count"),
            F.round(F.avg("policy_annual_premium"), 2).alias("avg_annual_premium"),
            F.round(F.sum("policy_annual_premium"), 2).alias("total_premium_revenue"),
            F.round(F.avg("policy_deductable"), 2).alias("avg_deductable"),
            F.round(F.avg("umbrella_limit"), 2).alias("avg_umbrella_limit"),
            F.sum(F.coalesce("claim_count", F.lit(0))).alias("total_claims_filed"),
            F.round(
                F.sum(F.coalesce("rejected_count", F.lit(0))) * 100.0 /
                F.greatest(F.sum(F.coalesce("claim_count", F.lit(0))), F.lit(1)), 2
            ).alias("overall_rejection_rate_pct"),
            F.round(F.sum(F.coalesce("total_claims_value", F.lit(0.0))), 2).alias("total_claims_exposure")
        )
        .orderBy("policy_csl")
    )