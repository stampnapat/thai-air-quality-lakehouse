# Databricks notebook source
# MAGIC %run ../notebooks/05_silver_dim_station

# COMMAND ----------

DEMO_TABLE = "air_quality.silver.dim_station_demo"
spark.sql(f"CREATE OR REPLACE TABLE {DEMO_TABLE} SHALLOW CLONE air_quality.silver.dim_station")
print("สร้างสำเนาแล้ว:", spark.table(DEMO_TABLE).count(), "แถว")

# COMMAND ----------

from pyspark.sql import functions as F

moved = (
    spark.table(DEMO_TABLE)
    .filter("station_id = 'o64' AND is_current")
    .select("station_id", "name_th", "name_en", "station_type")
    .withColumn("area_th", F.lit("ต.ศรีภูมิ อ.เมือง, เชียงใหม่"))
    .withColumn("area_en", F.lit("Si Phum, Mueang, Chiang Mai"))
    .withColumn("province_th", F.lit("เชียงใหม่"))
    .withColumn("lat", F.lit(18.7883))
    .withColumn("lon", F.lit(98.9853))
    .withColumn("valid_from", F.expr("to_timestamp_ntz('2026-11-25 09:15:00')"))
    .withColumn("attr_hash", F.sha2(F.to_json(F.struct(*TRACKED_COLS)), 256))
)

scd2_merge(moved, DEMO_TABLE)   # ฟังก์ชันตัวเดียวกับที่ pipeline จริงใช้

display(spark.table(DEMO_TABLE)
        .filter("station_id = 'o64'")
        .select("station_sk", "station_id", "province_th", "lat", "lon",
                "valid_from", "valid_to", "is_current")
        .orderBy("valid_from"))
print("ตารางจริงยังมี o64:", spark.table(DIM_TABLE).filter("station_id = 'o64'").count(), "แถว (ต้องเป็น 1)")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY air_quality.silver.dim_station_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT station_sk, station_id, province_th, valid_to, is_current
# MAGIC FROM air_quality.silver.dim_station_demo VERSION AS OF 0
# MAGIC WHERE station_id = 'o64'

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS air_quality.silver.dim_station_demo")
print("ลบสำเนาแล้ว")


# COMMAND ----------

# MAGIC %md
# MAGIC