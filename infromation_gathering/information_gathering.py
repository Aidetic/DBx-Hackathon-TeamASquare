# Databricks notebook source
# MAGIC %md
# MAGIC Query 1: Row count for every table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers_1' AS table_name, COUNT(*) AS row_count FROM enqurious_insurance.raw.customers_1
# MAGIC UNION ALL
# MAGIC SELECT 'customers_2', COUNT(*) FROM enqurious_insurance.raw.customers_2
# MAGIC UNION ALL
# MAGIC SELECT 'customers_3', COUNT(*) FROM enqurious_insurance.raw.customers_3
# MAGIC UNION ALL
# MAGIC SELECT 'customers_4', COUNT(*) FROM enqurious_insurance.raw.customers_4
# MAGIC UNION ALL
# MAGIC SELECT 'customers_5', COUNT(*) FROM enqurious_insurance.raw.customers_5
# MAGIC UNION ALL
# MAGIC SELECT 'customers_6', COUNT(*) FROM enqurious_insurance.raw.customers_6
# MAGIC UNION ALL
# MAGIC SELECT 'customers_7', COUNT(*) FROM enqurious_insurance.raw.customers_7
# MAGIC UNION ALL
# MAGIC SELECT 'policy', COUNT(*) FROM enqurious_insurance.raw.policy
# MAGIC UNION ALL
# MAGIC SELECT 'cars', COUNT(*) FROM enqurious_insurance.raw.cars
# MAGIC UNION ALL
# MAGIC SELECT 'sales_1', COUNT(*) FROM enqurious_insurance.raw.sales_1
# MAGIC UNION ALL
# MAGIC SELECT 'sales_2', COUNT(*) FROM enqurious_insurance.raw.sales_2
# MAGIC UNION ALL
# MAGIC SELECT 'sales_4', COUNT(*) FROM enqurious_insurance.raw.sales_4;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 2: Customer source schema difference view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers_1' AS table_name, 'CustomerID' AS id_col, 'Reg' AS region_col, 'City' AS city_col, 'Education' AS education_col, 'Marital_status' AS marital_col
# MAGIC UNION ALL
# MAGIC SELECT 'customers_2', 'Customer_ID', 'Region', 'City_in_state', 'Edu', NULL
# MAGIC UNION ALL
# MAGIC SELECT 'customers_3', 'cust_id', 'Region', 'City', 'Education', 'Marital'
# MAGIC UNION ALL
# MAGIC SELECT 'customers_4', 'CustomerID', 'Region', 'City', NULL, 'Marital'
# MAGIC UNION ALL
# MAGIC SELECT 'customers_5', 'CustomerID', 'Region', 'City', 'Education', 'Marital'
# MAGIC UNION ALL
# MAGIC SELECT 'customers_6', 'CustomerID', 'Region', 'City', 'Education', 'Marital_status'
# MAGIC UNION ALL
# MAGIC SELECT 'customers_7', 'CustomerID', 'Region', 'City', 'Education', 'Marital';

# COMMAND ----------

# MAGIC %md
# MAGIC Query 3: Null profiling for customer tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers_1' AS table_name,
# MAGIC        SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) AS null_customerid,
# MAGIC        SUM(CASE WHEN Reg IS NULL THEN 1 ELSE 0 END) AS null_region,
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS null_state,
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_city,
# MAGIC        SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END) AS null_education
# MAGIC FROM enqurious_insurance.raw.customers_1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_2',
# MAGIC        SUM(CASE WHEN Customer_ID IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City_in_state IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Edu IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM enqurious_insurance.raw.customers_2
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_3',
# MAGIC        SUM(CASE WHEN cust_id IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM enqurious_insurance.raw.customers_3
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_4',
# MAGIC        SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END),
# MAGIC        NULL
# MAGIC FROM enqurious_insurance.raw.customers_4
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_5',
# MAGIC        SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM enqurious_insurance.raw.customers_5
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_6',
# MAGIC        SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM enqurious_insurance.raw.customers_6
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_7',
# MAGIC        SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN Education IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM enqurious_insurance.raw.customers_7;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 4: Find region inconsistencies in customer sources

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers_1' AS table_name, Reg AS region_value, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.customers_1
# MAGIC GROUP BY Reg
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_2', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_2
# MAGIC GROUP BY Region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_3', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_3
# MAGIC GROUP BY Region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_4', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_4
# MAGIC GROUP BY Region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_5', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_5
# MAGIC GROUP BY Region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_6', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_6
# MAGIC GROUP BY Region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_7', Region, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_7
# MAGIC GROUP BY Region
# MAGIC ORDER BY table_name, region_value;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 5: Find bad education values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers_1' AS table_name, Education AS education_value, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.customers_1
# MAGIC GROUP BY Education
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_2', Edu, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_2
# MAGIC GROUP BY Edu
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_3', Education, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_3
# MAGIC GROUP BY Education
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_5', Education, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_5
# MAGIC GROUP BY Education
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_6', Education, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_6
# MAGIC GROUP BY Education
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customers_7', Education, COUNT(*)
# MAGIC FROM enqurious_insurance.raw.customers_7
# MAGIC GROUP BY Education
# MAGIC ORDER BY table_name, education_value;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 6: Prove customers_6 has swapped columns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Education, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.customers_6
# MAGIC GROUP BY Education
# MAGIC ORDER BY cnt DESC;
# MAGIC
# MAGIC SELECT Marital_status, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.customers_6
# MAGIC GROUP BY Marital_status
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 7: Create one harmonized customer exploration view

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_union AS (
# MAGIC     SELECT
# MAGIC         'customers_1' AS source_table,
# MAGIC         CustomerID AS customer_id,
# MAGIC         Reg AS region,
# MAGIC         State,
# MAGIC         City,
# MAGIC         CAST(NULL AS STRING) AS Job,
# MAGIC         Marital_status AS marital_status,
# MAGIC         Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_1
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'customers_2',
# MAGIC         Customer_ID,
# MAGIC         Region,
# MAGIC         State,
# MAGIC         City_in_state AS City,
# MAGIC         Job,
# MAGIC         CAST(NULL AS STRING) AS marital_status,
# MAGIC         Edu AS Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         CAST(NULL AS INT) AS HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_2
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'customers_3',
# MAGIC         cust_id,
# MAGIC         Region,
# MAGIC         State,
# MAGIC         City,
# MAGIC         Job,
# MAGIC         Marital,
# MAGIC         Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_3
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'customers_4',
# MAGIC         CustomerID,
# MAGIC         Region,
# MAGIC         State,
# MAGIC         City,
# MAGIC         Job,
# MAGIC         Marital,
# MAGIC         CAST(NULL AS STRING) AS Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_4
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'customers_5',
# MAGIC         CustomerID,
# MAGIC         CASE
# MAGIC             WHEN Region = 'E' THEN 'East'
# MAGIC             WHEN Region = 'W' THEN 'West'
# MAGIC             WHEN Region = 'C' THEN 'Central'
# MAGIC             WHEN Region = 'S' THEN 'South'
# MAGIC             ELSE Region
# MAGIC         END AS Region,
# MAGIC         State,
# MAGIC         City,
# MAGIC         Job,
# MAGIC         Marital,
# MAGIC         CASE WHEN Education = 'terto' THEN 'tertiary' ELSE Education END AS Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_5
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'customers_6',
# MAGIC         CustomerID,
# MAGIC         Region,
# MAGIC         State,
# MAGIC         City,
# MAGIC         Job,
# MAGIC         Education AS marital_status,
# MAGIC         Marital_status AS Education,
# MAGIC         Default,
# MAGIC         Balance,
# MAGIC         HHInsurance,
# MAGIC         CarLoan
# MAGIC     FROM enqurious_insurance.raw.customers_6
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM customer_union;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 8: Count customers by source after harmonization

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_union AS (
# MAGIC     SELECT 'customers_1' AS source_table, CustomerID AS customer_id FROM enqurious_insurance.raw.customers_1
# MAGIC     UNION ALL
# MAGIC     SELECT 'customers_2', Customer_ID FROM enqurious_insurance.raw.customers_2
# MAGIC     UNION ALL
# MAGIC     SELECT 'customers_3', cust_id FROM enqurious_insurance.raw.customers_3
# MAGIC     UNION ALL
# MAGIC     SELECT 'customers_4', CustomerID FROM enqurious_insurance.raw.customers_4
# MAGIC     UNION ALL
# MAGIC     SELECT 'customers_5', CustomerID FROM enqurious_insurance.raw.customers_5
# MAGIC     UNION ALL
# MAGIC     SELECT 'customers_6', CustomerID FROM enqurious_insurance.raw.customers_6
# MAGIC )
# MAGIC SELECT source_table,
# MAGIC        COUNT(*) AS rows_in_source,
# MAGIC        COUNT(DISTINCT customer_id) AS distinct_customers
# MAGIC FROM customer_union
# MAGIC GROUP BY source_table
# MAGIC ORDER BY source_table;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 9: Check whether customers_7 is consolidated

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH source_1_6 AS (
# MAGIC     SELECT CustomerID AS customer_id FROM enqurious_insurance.raw.customers_1
# MAGIC     UNION
# MAGIC     SELECT Customer_ID FROM enqurious_insurance.raw.customers_2
# MAGIC     UNION
# MAGIC     SELECT cust_id FROM enqurious_insurance.raw.customers_3
# MAGIC     UNION
# MAGIC     SELECT CustomerID FROM enqurious_insurance.raw.customers_4
# MAGIC     UNION
# MAGIC     SELECT CustomerID FROM enqurious_insurance.raw.customers_5
# MAGIC     UNION
# MAGIC     SELECT CustomerID FROM enqurious_insurance.raw.customers_6
# MAGIC )
# MAGIC SELECT c7.CustomerID
# MAGIC FROM enqurious_insurance.raw.customers_7 c7
# MAGIC LEFT ANTI JOIN source_1_6 s
# MAGIC ON c7.CustomerID = s.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 10: Policy null and key profiling

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT policy_number) AS distinct_policy_number,
# MAGIC     COUNT(DISTINCT customer_id) AS distinct_customer_id,
# MAGIC     COUNT(DISTINCT car_id) AS distinct_car_id,
# MAGIC     SUM(CASE WHEN policy_number IS NULL THEN 1 ELSE 0 END) AS null_policy_number,
# MAGIC     SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
# MAGIC     SUM(CASE WHEN car_id IS NULL THEN 1 ELSE 0 END) AS null_car_id,
# MAGIC     SUM(CASE WHEN policy_bind_date IS NULL THEN 1 ELSE 0 END) AS null_policy_bind_date
# MAGIC FROM enqurious_insurance.raw.policy;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 11: Suspicious policy business values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM enqurious_insurance.raw.policy
# MAGIC WHERE umbrella_limit < 0
# MAGIC    OR policy_annual_premium <= 0
# MAGIC    OR policy_deductable <= 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 12: Policy to customer referential integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.*
# MAGIC FROM enqurious_insurance.raw.policy p
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.customers_7 c
# MAGIC ON p.customer_id = c.CustomerID;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 13: Policy to cars referential integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.*
# MAGIC FROM enqurious_insurance.raw.policy p
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.cars c
# MAGIC ON p.car_id = c.car_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 14: Sales blank row detection

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM enqurious_insurance.raw.sales_1
# MAGIC WHERE sales_id IS NULL
# MAGIC   AND ad_placed_on IS NULL
# MAGIC   AND sold_on IS NULL
# MAGIC   AND original_selling_price IS NULL
# MAGIC   AND Region IS NULL
# MAGIC   AND State IS NULL
# MAGIC   AND City IS NULL
# MAGIC   AND seller_type IS NULL
# MAGIC   AND owner IS NULL
# MAGIC   AND car_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 15: Sales partially corrupted rows

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM enqurious_insurance.raw.sales_1
# MAGIC WHERE ad_placed_on IS NOT NULL
# MAGIC   AND (
# MAGIC         sales_id IS NULL
# MAGIC      OR original_selling_price IS NULL
# MAGIC      OR Region IS NULL
# MAGIC      OR State IS NULL
# MAGIC      OR City IS NULL
# MAGIC      OR seller_type IS NULL
# MAGIC      OR owner IS NULL
# MAGIC      OR car_id IS NULL
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC Query 16: Cleaned sales counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'sales_1' AS table_name, COUNT(*) AS usable_rows
# MAGIC FROM enqurious_insurance.raw.sales_1
# MAGIC WHERE NOT (
# MAGIC     sales_id IS NULL
# MAGIC     AND ad_placed_on IS NULL
# MAGIC     AND sold_on IS NULL
# MAGIC     AND original_selling_price IS NULL
# MAGIC     AND Region IS NULL
# MAGIC     AND State IS NULL
# MAGIC     AND City IS NULL
# MAGIC     AND seller_type IS NULL
# MAGIC     AND owner IS NULL
# MAGIC     AND car_id IS NULL
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'sales_2', COUNT(*)
# MAGIC FROM enqurious_insurance.raw.sales_2
# MAGIC WHERE NOT (
# MAGIC     sales_id IS NULL
# MAGIC     AND ad_placed_on IS NULL
# MAGIC     AND sold_on IS NULL
# MAGIC     AND original_selling_price IS NULL
# MAGIC     AND Region IS NULL
# MAGIC     AND State IS NULL
# MAGIC     AND City IS NULL
# MAGIC     AND seller_type IS NULL
# MAGIC     AND owner IS NULL
# MAGIC     AND car_id IS NULL
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'sales_4', COUNT(*)
# MAGIC FROM enqurious_insurance.raw.sales_4
# MAGIC WHERE NOT (
# MAGIC     sales_id IS NULL
# MAGIC     AND ad_placed_on IS NULL
# MAGIC     AND sold_on IS NULL
# MAGIC     AND original_selling_price IS NULL
# MAGIC     AND Region IS NULL
# MAGIC     AND State IS NULL
# MAGIC     AND City IS NULL
# MAGIC     AND seller_type IS NULL
# MAGIC     AND owner IS NULL
# MAGIC     AND car_id IS NULL
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Query 17: Unsold vehicles by sales table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'sales_1' AS table_name, COUNT(*) AS unsold_count
# MAGIC FROM enqurious_insurance.raw.sales_1
# MAGIC WHERE sold_on IS NULL
# MAGIC   AND car_id IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'sales_2', COUNT(*)
# MAGIC FROM enqurious_insurance.raw.sales_2
# MAGIC WHERE sold_on IS NULL
# MAGIC   AND car_id IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'sales_4', COUNT(*)
# MAGIC FROM enqurious_insurance.raw.sales_4
# MAGIC WHERE sold_on IS NULL
# MAGIC   AND car_id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 18: Aging unsold inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales_all AS (
# MAGIC     SELECT 'sales_1' AS source_table, car_id, Region, State, City,
# MAGIC            to_timestamp(ad_placed_on, 'dd-MM-yyyy HH:mm') AS ad_placed_ts,
# MAGIC            to_timestamp(sold_on, 'dd-MM-yyyy HH:mm') AS sold_ts
# MAGIC     FROM enqurious_insurance.raw.sales_1
# MAGIC     WHERE car_id IS NOT NULL
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'sales_2', car_id, Region, State, City,
# MAGIC            to_timestamp(ad_placed_on, 'dd-MM-yyyy HH:mm'),
# MAGIC            to_timestamp(sold_on, 'dd-MM-yyyy HH:mm')
# MAGIC     FROM enqurious_insurance.raw.sales_2
# MAGIC     WHERE car_id IS NOT NULL
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'sales_4', car_id, Region, State, City,
# MAGIC            to_timestamp(ad_placed_on, 'dd-MM-yyyy HH:mm'),
# MAGIC            to_timestamp(sold_on, 'dd-MM-yyyy HH:mm')
# MAGIC     FROM enqurious_insurance.raw.sales_4
# MAGIC     WHERE car_id IS NOT NULL
# MAGIC )
# MAGIC SELECT source_table,
# MAGIC        Region,
# MAGIC        COUNT(*) AS unsold_cars,
# MAGIC        SUM(CASE WHEN datediff(current_date(), CAST(ad_placed_ts AS DATE)) >= 90 THEN 1 ELSE 0 END) AS unsold_90_plus_days,
# MAGIC        SUM(CASE WHEN datediff(current_date(), CAST(ad_placed_ts AS DATE)) >= 60 THEN 1 ELSE 0 END) AS unsold_60_plus_days
# MAGIC FROM sales_all
# MAGIC WHERE sold_ts IS NULL
# MAGIC GROUP BY source_table, Region
# MAGIC ORDER BY source_table, Region;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 19: Overlap / duplicate sale across files

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales_all AS (
# MAGIC     SELECT 'sales_1' AS source_table, sales_id, car_id
# MAGIC     FROM enqurious_insurance.raw.sales_1
# MAGIC     WHERE sales_id IS NOT NULL AND car_id IS NOT NULL
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'sales_2', sales_id, car_id
# MAGIC     FROM enqurious_insurance.raw.sales_2
# MAGIC     WHERE sales_id IS NOT NULL AND car_id IS NOT NULL
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'sales_4', sales_id, car_id
# MAGIC     FROM enqurious_insurance.raw.sales_4
# MAGIC     WHERE sales_id IS NOT NULL AND car_id IS NOT NULL
# MAGIC )
# MAGIC SELECT sales_id, car_id, COUNT(*) AS record_count,
# MAGIC        collect_set(source_table) AS source_tables
# MAGIC FROM sales_all
# MAGIC GROUP BY sales_id, car_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 20: Cars quality profile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT car_id) AS distinct_car_id,
# MAGIC     SUM(CASE WHEN car_id IS NULL THEN 1 ELSE 0 END) AS null_car_id,
# MAGIC     SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name,
# MAGIC     SUM(CASE WHEN fuel IS NULL THEN 1 ELSE 0 END) AS null_fuel,
# MAGIC     SUM(CASE WHEN transmission IS NULL THEN 1 ELSE 0 END) AS null_transmission,
# MAGIC     SUM(CASE WHEN mileage IS NULL THEN 1 ELSE 0 END) AS null_mileage,
# MAGIC     SUM(CASE WHEN engine IS NULL THEN 1 ELSE 0 END) AS null_engine,
# MAGIC     SUM(CASE WHEN max_power IS NULL THEN 1 ELSE 0 END) AS null_max_power,
# MAGIC     SUM(CASE WHEN torque IS NULL THEN 1 ELSE 0 END) AS null_torque
# MAGIC FROM enqurious_insurance.raw.cars;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 21: Cars distribution overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fuel, transmission, seats, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.cars
# MAGIC GROUP BY fuel, transmission, seats
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 22: Sales to cars referential integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales_all AS (
# MAGIC     SELECT 'sales_1' AS source_table, car_id FROM enqurious_insurance.raw.sales_1 WHERE car_id IS NOT NULL
# MAGIC     UNION ALL
# MAGIC     SELECT 'sales_2', car_id FROM enqurious_insurance.raw.sales_2 WHERE car_id IS NOT NULL
# MAGIC     UNION ALL
# MAGIC     SELECT 'sales_4', car_id FROM enqurious_insurance.raw.sales_4 WHERE car_id IS NOT NULL
# MAGIC )
# MAGIC SELECT s.*
# MAGIC FROM sales_all s
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.cars c
# MAGIC ON s.car_id = c.car_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 23: Policy + customer + car business join preview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.policy_number,
# MAGIC     p.policy_bind_date,
# MAGIC     p.policy_state,
# MAGIC     p.policy_annual_premium,
# MAGIC     p.customer_id,
# MAGIC     c.Region,
# MAGIC     c.State,
# MAGIC     c.City,
# MAGIC     c.Job,
# MAGIC     c.Marital,
# MAGIC     c.Education,
# MAGIC     p.car_id,
# MAGIC     car.name AS car_name,
# MAGIC     car.model,
# MAGIC     car.fuel,
# MAGIC     car.transmission
# MAGIC FROM enqurious_insurance.raw.policy p
# MAGIC LEFT JOIN enqurious_insurance.raw.customers_7 c
# MAGIC     ON p.customer_id = c.CustomerID
# MAGIC LEFT JOIN enqurious_insurance.raw.cars car
# MAGIC     ON p.car_id = car.car_id
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 24: Claims row count and null profile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN ClaimID IS NULL THEN 1 ELSE 0 END) AS null_claimid,
# MAGIC     SUM(CASE WHEN Claim_Logged_On IS NULL THEN 1 ELSE 0 END) AS null_claim_logged_on
# MAGIC FROM enqurious_insurance.raw.claims_1;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 25: Claims schema inspection

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE enqurious_insurance.raw.claims_1;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 26: Claims duplicate check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ClaimID, COUNT(*) AS cnt
# MAGIC FROM enqurious_insurance.raw.claims_1
# MAGIC GROUP BY ClaimID
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 27: Claims date quality

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM enqurious_insurance.raw.claims_1
# MAGIC WHERE Claim_Logged_On IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ClaimID,
# MAGIC     Claim_Logged_On,
# MAGIC     --to_timestamp(Claim_Logged_On, 'dd-MM-yyyy HH:mm') AS parsed_claim_ts
# MAGIC     Claim_Logged_On
# MAGIC FROM enqurious_insurance.raw.claims_1;

# COMMAND ----------

# MAGIC %md
# MAGIC Query 28: Claims joinability to policy/customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cl.*
# MAGIC FROM enqurious_insurance.raw.claims_1 cl
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.policy p
# MAGIC ON cl.policy_number = p.policy_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cl.*
# MAGIC FROM enqurious_insurance.raw.claims_1 cl
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.policy p
# MAGIC ON cl.PolicyID = p.policy_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cl.*
# MAGIC FROM enqurious_insurance.raw.claims_1 cl
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.customers_7 c
# MAGIC ON cl.customer_id = c.CustomerID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cl.*
# MAGIC FROM enqurious_insurance.raw.claims_1 cl
# MAGIC LEFT ANTI JOIN enqurious_insurance.raw.customers_7 c
# MAGIC ON cl.ClaimID = c.CustomerID;

# COMMAND ----------

