# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "air_quality"
BRONZE_TABLE = f"{CATALOG}.bronze.air4thai_aqi"
SILVER_TABLE = f"{CATALOG}.silver.aqi_hourly"
CHECKPOINT = f"/Volumes/{CATALOG}/silver/checkpoints/aqi_hourly"
STALE_MINUTES = 180  # ค่าที่เก่าเกิน 3 ชม. ตอนดึงมา = สถานีค้าง


# 1) กฎทำความสะอาด ---------------------------------------------------------
def to_number(col_name, dtype="double"):
    """แปลงข้อความเป็นตัวเลข ถ้าติดลบ (เช่น -1) หรือแปลงไม่ได้ ให้เป็น NULL"""
    v = F.col(col_name).cast(dtype)
    return F.when(v >= 0, v)


def clean_bronze(df):
    """Bronze (ข้อความดิบ) -> แถวที่สะอาด 1 แถวต่อ 1 การวัด"""
    return (
        df.select(
            F.col("stationID").alias("station_id"),
            F.expr("to_timestamp_ntz(concat(AQILast.date, ' ', AQILast.time), 'yyyy-MM-dd HH:mm')").alias("measured_at"),
            F.to_date("AQILast.date").alias("measured_date"),
            to_number("AQILast.PM25.value").alias("pm25"),
            to_number("AQILast.PM25.aqi", "int").alias("pm25_aqi"),
            to_number("AQILast.PM10.value").alias("pm10"),
            to_number("AQILast.O3.value").alias("o3"),
            to_number("AQILast.CO.value").alias("co"),
            to_number("AQILast.NO2.value").alias("no2"),
            to_number("AQILast.SO2.value").alias("so2"),
            to_number("AQILast.AQI.aqi", "int").alias("aqi"),
            F.col("AQILast.AQI.param").alias("aqi_param"),
            F.regexp_extract("_source_file", r"(\d{8}_\d{6})", 1).alias("fetched_str"),
            "_source_file",
        )
        .withColumn("fetched_at", F.expr("to_timestamp_ntz(fetched_str, 'yyyyMMdd_HHmmss')"))
        .withColumn("fetch_lag_minutes", F.expr("CAST(timestampdiff(MINUTE, measured_at, fetched_at) AS INT)"))
        .withColumn("is_stale", F.col("fetch_lag_minutes") > STALE_MINUTES)
        .withColumn("row_hash", F.sha2(F.to_json(F.struct(
            "pm25", "pm25_aqi", "pm10", "o3", "co", "no2", "so2", "aqi", "aqi_param")), 256))
        .filter(F.col("station_id").isNotNull() & F.col("measured_at").isNotNull())
        .drop("fetched_str")
    )


def dedup(df):
    """เก็บ 1 แถวต่อ (สถานี, เวลาที่วัด) เลือกครั้งแรกที่ดึงมาได้"""
    w = Window.partitionBy("station_id", "measured_at").orderBy(F.col("fetched_at").asc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


# 2) เขียนลง Silver แบบ MERGE (ทำทุกรอบที่มีข้อมูลใหม่จาก Bronze) -----------
def upsert_to_silver(batch_df, batch_id):
    updates = dedup(batch_df).withColumn("_updated_at", F.current_timestamp())
    updates.createOrReplaceTempView("silver_updates")
    updates.sparkSession.sql(f"""
        MERGE INTO {SILVER_TABLE} AS t
        USING silver_updates AS s
        ON t.station_id = s.station_id AND t.measured_at = s.measured_at
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# 3) สร้างตาราง + รัน ------------------------------------------------------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.silver.checkpoints")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  station_id        STRING NOT NULL,
  measured_at       TIMESTAMP_NTZ NOT NULL COMMENT 'เวลาที่วัด (เวลาไทย)',
  measured_date     DATE,
  pm25              DOUBLE COMMENT 'ug/m3, NULL = ไม่มีค่า',
  pm25_aqi          INT,
  pm10              DOUBLE,
  o3                DOUBLE,
  co                DOUBLE,
  no2               DOUBLE,
  so2               DOUBLE,
  aqi               INT,
  aqi_param         STRING,
  _source_file      STRING,
  fetched_at        TIMESTAMP_NTZ COMMENT 'เวลาที่ดึงมาได้ครั้งแรก (เวลาไทย)',
  fetch_lag_minutes INT,
  is_stale          BOOLEAN COMMENT 'TRUE = ค่าเก่าเกิน 3 ชม. ตอนดึงมา',
  row_hash          STRING,
  _updated_at       TIMESTAMP
)
""")

query = (
    spark.readStream.table(BRONZE_TABLE)
    .transform(clean_bronze)
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print("เสร็จแล้ว เขียนลง", SILVER_TABLE)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_n = spark.table("air_quality.bronze.air4thai_aqi").count()
silver = spark.table("air_quality.silver.aqi_hourly")
silver_n = silver.count()
print(f"Bronze: {bronze_n:,} แถว  →  Silver: {silver_n:,} แถว  (ลบซ้ำไป {bronze_n - silver_n:,} แถว)")

dups = silver.groupBy("station_id", "measured_at").count().filter("count > 1").count()
print("ข้อมูลซ้ำใน Silver:", dups, "(ต้องเป็น 0)")
print("สถานีที่ค่าค้าง:", silver.filter("is_stale").select("station_id").distinct().count())

display(silver.orderBy(F.desc("measured_at"), "station_id")
        .select("station_id", "measured_at", "pm25", "pm10", "aqi", "aqi_param",
                "fetched_at", "fetch_lag_minutes", "is_stale")
        .limit(20))
