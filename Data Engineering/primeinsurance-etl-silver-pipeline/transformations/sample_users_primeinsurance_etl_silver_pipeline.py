import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import FloatType, IntegerType

BRONZE = "primeinsurance.bronze"


# ─────────────────────────────────────────────────────────────
# CUSTOMERS
# ─────────────────────────────────────────────────────────────

def _transform_customers():
    df = spark.read.table(f"{BRONZE}.customers")

    # Three ID column variants across source files — unify into one
    df = df.withColumn("customer_id",
        F.coalesce(F.col("CustomerID"), F.col("Customer_ID"), F.col("cust_id"))
    )

    # Region comes as full names or single-letter abbreviations depending on source
    df = df.withColumn("region_raw",
        F.coalesce(F.col("Region"), F.col("Reg"))
    ).withColumn("region",
        F.when(F.col("region_raw") == "E", "East")
         .when(F.col("region_raw") == "W", "West")
         .when(F.col("region_raw") == "C", "Central")
         .when(F.col("region_raw") == "S", "South")
         .otherwise(F.col("region_raw"))
    )

    df = df.withColumn("city",
        F.coalesce(F.col("City"), F.col("City_in_state"))
    )

    # customers_6 has Education and Marital_status columns swapped at source
    # Detect the swap by checking if Education column actually contains marital values
    df = df.withColumn("marital",
        F.when(
            F.col("Education").isin("single", "married", "divorced"),
            F.col("Education")
        ).otherwise(
            F.coalesce(F.col("Marital"), F.col("Marital_status"))
        )
    )

    df = df.withColumn("education_raw",
        F.when(
            F.col("Marital_status").isin("primary", "secondary", "tertiary", "NA"),
            F.col("Marital_status")
        ).otherwise(
            F.coalesce(F.col("Education"), F.col("Edu"))
        )
    ).withColumn("education",
        F.when(F.col("education_raw") == "terto", "tertiary")
         .otherwise(F.col("education_raw"))
    )

    df = df.select(
        F.col("customer_id"),
        F.col("region"),
        F.col("State").alias("state"),
        F.col("city"),
        F.col("Job").alias("job"),
        F.col("marital"),
        F.col("education"),
        F.col("Default").cast(IntegerType()).alias("default_flag"),
        F.col("Balance").cast(IntegerType()).alias("balance"),
        F.col("HHInsurance").cast(IntegerType()).alias("hh_insurance"),
        F.col("CarLoan").cast(IntegerType()).alias("car_loan"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )

    # Deduplicate — customers_7 is the master file, give it priority
    window = Window.partitionBy("customer_id").orderBy(
        F.when(F.col("_source_file").contains("customers_7"), 0).otherwise(1),
        F.col("_source_file")
    )
    df = df.withColumn("_rn", F.row_number().over(window)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    return df


@dlt.table(
    name="customers",
    comment="Harmonized customer master — deduplicated, standardized, quality-enforced",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region IN ('East','West','Central','South','North')")
@dlt.expect_or_drop("valid_marital", "marital IN ('single','married','divorced') OR marital IS NULL")
@dlt.expect_or_drop("valid_education", "education IN ('primary','secondary','tertiary','NA') OR education IS NULL")
def silver_customers():
    return _transform_customers()


@dlt.table(
    name="customers_failed",
    comment="Customer records that failed Silver quality checks",
    table_properties={"quality": "quarantine"}
)
def quarantine_customers():
    df = _transform_customers()
    return df.filter(
        F.col("customer_id").isNull() |
        ~F.col("region").isin("East", "West", "Central", "South", "North") |
        (F.col("marital").isNotNull() & ~F.col("marital").isin("single", "married", "divorced")) |
        (F.col("education").isNotNull() & ~F.col("education").isin("primary", "secondary", "tertiary", "NA"))
    ).withColumn("_rejection_reason",
        F.concat_ws(", ",
            F.when(F.col("customer_id").isNull(), F.lit("null customer_id")),
            F.when(~F.col("region").isin("East","West","Central","South","North"),
                   F.lit("invalid region: " + F.col("region").cast("string"))),
            F.when(F.col("marital").isNotNull() & ~F.col("marital").isin("single","married","divorced"),
                   F.lit("invalid marital")),
            F.when(F.col("education").isNotNull() & ~F.col("education").isin("primary","secondary","tertiary","NA"),
                   F.lit("invalid education"))
        )
    )


# ─────────────────────────────────────────────────────────────
# CLAIMS
# ─────────────────────────────────────────────────────────────

def _transform_claims():
    df = spark.read.table(f"{BRONZE}.claims")

    return df.select(
        F.col("ClaimID").alias("claim_id"),
        F.col("PolicyID").alias("policy_id"),
        F.col("incident_state"),
        F.col("incident_city"),
        F.col("incident_location"),
        F.col("incident_type"),
        F.col("collision_type"),
        F.col("incident_severity"),
        F.col("authorities_contacted"),
        F.when(F.col("injury").isin("NULL", ""), None)
         .otherwise(F.col("injury"))
         .cast(FloatType()).alias("injury_amount"),
        F.when(F.col("property").isin("NULL", ""), None)
         .otherwise(F.col("property"))
         .cast(FloatType()).alias("property_amount"),
        # 29 records have "NULL" string for vehicle — actual nulls, not data errors
        F.when(
            (F.col("vehicle") == "NULL") | F.col("vehicle").isNull() | (F.col("vehicle") == ""),
            None
        ).otherwise(F.col("vehicle"))
         .cast(FloatType()).alias("vehicle_amount"),
        F.col("number_of_vehicles_involved").cast(IntegerType()),
        F.when(F.col("property_damage") == "?", None)
         .otherwise(F.col("property_damage")).alias("property_damage"),
        F.when(F.col("police_report_available") == "?", None)
         .otherwise(F.col("police_report_available")).alias("police_report_available"),
        F.col("bodily_injuries").cast(IntegerType()),
        F.col("witnesses").cast(IntegerType()),
        F.col("Claim_Rejected").alias("claim_rejected"),
        F.when(F.col("Claim_Processed_On").isin("NULL", ""), None)
         .otherwise(F.col("Claim_Processed_On")).alias("claim_processed_on_raw"),
        # incident_date and Claim_Logged_On are corrupted at source (27:00.0 format)
        # Keeping raw values for audit; parsed timestamps are unrecoverable
        F.col("incident_date").alias("incident_date_raw"),
        F.col("Claim_Logged_On").alias("claim_logged_on_raw"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


@dlt.table(
    name="claims",
    comment="Harmonized claims — type-corrected, nulls standardized, quality-enforced",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_claim_id",  "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_drop("valid_rejected",  "claim_rejected IN ('Y','N')")
@dlt.expect_or_drop("valid_injury",    "injury_amount IS NOT NULL")
@dlt.expect_or_drop("valid_property",  "property_amount IS NOT NULL")
def silver_claims():
    return _transform_claims()


@dlt.table(
    name="claims_failed",
    comment="Claims that failed Silver quality checks",
    table_properties={"quality": "quarantine"}
)
def quarantine_claims():
    df = _transform_claims()
    # vehicle_amount NULLs are not quarantined — confirmed valid claims
    return df.filter(
        F.col("claim_id").isNull() |
        F.col("policy_id").isNull() |
        ~F.col("claim_rejected").isin("Y", "N") |
        F.col("injury_amount").isNull() |
        F.col("property_amount").isNull()
    ).withColumn("_rejection_reason",
        F.concat_ws(", ",
            F.when(F.col("claim_id").isNull(), F.lit("null claim_id")),
            F.when(F.col("policy_id").isNull(), F.lit("null policy_id")),
            F.when(~F.col("claim_rejected").isin("Y","N"), F.lit("invalid claim_rejected")),
            F.when(F.col("injury_amount").isNull(), F.lit("injury not castable to float")),
            F.when(F.col("property_amount").isNull(), F.lit("property not castable to float"))
        )
    )


# ─────────────────────────────────────────────────────────────
# POLICY
# ─────────────────────────────────────────────────────────────

def _transform_policy():
    df = spark.read.table(f"{BRONZE}.policy")

    return df.select(
        F.col("policy_number"),
        F.to_date(F.col("policy_bind_date"), "yyyy-MM-dd").alias("policy_bind_date"),
        F.col("policy_state"),
        F.col("policy_csl"),
        F.col("policy_deductable").cast(IntegerType()).alias("policy_deductable"),
        F.col("policy_annual_premium").cast(FloatType()).alias("policy_annual_premium"),
        F.col("umbrella_limit").cast(IntegerType()).alias("umbrella_limit"),
        F.col("car_id"),
        F.col("customer_id"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


@dlt.table(
    name="policy",
    comment="Harmonized policy records — types cast, date parsed, quality-enforced",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_policy_number", "policy_number IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id",   "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_car_id",        "car_id IS NOT NULL")
@dlt.expect_or_drop("valid_premium",       "policy_annual_premium > 0")
@dlt.expect_or_drop("valid_umbrella",      "umbrella_limit >= 0")
def silver_policy():
    return _transform_policy()


@dlt.table(
    name="policy_failed",
    comment="Policy records that failed Silver quality checks",
    table_properties={"quality": "quarantine"}
)
def quarantine_policy():
    df = _transform_policy()
    return df.filter(
        F.col("policy_number").isNull() |
        F.col("customer_id").isNull() |
        F.col("car_id").isNull() |
        (F.col("policy_annual_premium") <= 0) |
        (F.col("umbrella_limit") < 0)
    ).withColumn("_rejection_reason",
        F.concat_ws(", ",
            F.when(F.col("policy_number").isNull(), F.lit("null policy_number")),
            F.when(F.col("customer_id").isNull(), F.lit("null customer_id")),
            F.when(F.col("car_id").isNull(), F.lit("null car_id")),
            F.when(F.col("policy_annual_premium") <= 0, F.lit("invalid premium")),
            F.when(F.col("umbrella_limit") < 0, F.lit("negative umbrella_limit"))
        )
    )


# ─────────────────────────────────────────────────────────────
# SALES
# ─────────────────────────────────────────────────────────────

def _transform_sales():
    df = spark.read.table(f"{BRONZE}.sales")

    # Drop blank padding rows introduced by CSV export (sales_id will be null for these)
    df = df.filter(F.col("sales_id").isNotNull())

    return df.select(
        F.col("sales_id").cast(IntegerType()),
        F.to_timestamp(F.col("ad_placed_on"), "dd-MM-yyyy HH:mm").alias("ad_placed_on"),
        F.to_timestamp(F.col("sold_on"), "dd-MM-yyyy HH:mm").alias("sold_on"),
        F.col("original_selling_price").cast(FloatType()),
        F.col("Region").alias("region"),
        F.col("State").alias("state"),
        F.col("City").alias("city"),
        F.col("seller_type"),
        F.col("owner"),
        F.col("car_id"),
        # NULL when car is sold; days since listing when still active
        F.when(
            F.col("sold_on").isNotNull(),
            F.lit(None).cast(IntegerType())
        ).otherwise(
            F.datediff(F.current_date(), F.to_date(F.col("ad_placed_on"), "dd-MM-yyyy HH:mm"))
        ).alias("days_unsold"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


@dlt.table(
    name="sales",
    comment="Harmonized sales — blank rows removed, dates parsed, days_unsold computed",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_sales_id",  "sales_id IS NOT NULL")
@dlt.expect_or_drop("valid_ad_placed", "ad_placed_on IS NOT NULL")
@dlt.expect_or_drop("valid_car_id",    "car_id IS NOT NULL")
def silver_sales():
    return _transform_sales()


@dlt.table(
    name="sales_failed",
    comment="Sales records that failed Silver quality checks",
    table_properties={"quality": "quarantine"}
)
def quarantine_sales():
    df = _transform_sales()
    return df.filter(
        F.col("sales_id").isNull() |
        F.col("ad_placed_on").isNull() |
        F.col("car_id").isNull()
    ).withColumn("_rejection_reason",
        F.concat_ws(", ",
            F.when(F.col("sales_id").isNull(), F.lit("null sales_id")),
            F.when(F.col("ad_placed_on").isNull(), F.lit("null ad_placed_on")),
            F.when(F.col("car_id").isNull(), F.lit("null car_id"))
        )
    )


# ─────────────────────────────────────────────────────────────
# CARS
# ─────────────────────────────────────────────────────────────

def _transform_cars():
    df = spark.read.table(f"{BRONZE}.cars")

    return df.select(
        F.col("car_id"),
        F.col("name"),
        F.col("km_driven").cast(IntegerType()),
        F.col("fuel"),
        F.col("transmission"),
        # Strip unit suffixes and extract numeric value
        F.regexp_extract(F.col("mileage"), r"([\d.]+)", 1).cast(FloatType()).alias("mileage_kmpl"),
        F.regexp_extract(F.col("engine"), r"([\d.]+)", 1).cast(FloatType()).alias("engine_cc"),
        F.regexp_extract(F.col("max_power"), r"([\d.]+)", 1).cast(FloatType()).alias("max_power_bhp"),
        # torque format is inconsistent across sources — keeping raw
        F.col("torque").alias("torque_raw"),
        F.col("seats").cast(IntegerType()),
        F.col("model"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


@dlt.table(
    name="cars",
    comment="Harmonized vehicles — unit suffixes stripped, numeric types corrected",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_car_id", "car_id IS NOT NULL")
@dlt.expect_or_drop("valid_seats",  "seats > 0")
def silver_cars():
    return _transform_cars()


@dlt.table(
    name="cars_failed",
    comment="Car records that failed Silver quality checks",
    table_properties={"quality": "quarantine"}
)
def quarantine_cars():
    df = _transform_cars()
    return df.filter(
        F.col("car_id").isNull() |
        (F.col("seats") <= 0)
    ).withColumn("_rejection_reason",
        F.concat_ws(", ",
            F.when(F.col("car_id").isNull(), F.lit("null car_id")),
            F.when(F.col("seats") <= 0, F.lit("invalid seats"))
        )
    )