# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "air_quality")  # job parameter; dev target uses air_quality_dev
CATALOG = dbutils.widgets.get("catalog")
BRONZE_TABLE = f"{CATALOG}.bronze.air4thai_aqi"
DIM_TABLE = f"{CATALOG}.silver.dim_station"
CHECKPOINT = f"/Volumes/{CATALOG}/silver/checkpoints/dim_station"

# คอลัมน์ที่ถ้าเปลี่ยน = สร้างเวอร์ชันใหม่ (SCD Type 2)
TRACKED_COLS = ["name_th", "name_en", "area_th", "area_en",
                "province_th", "station_type", "lat", "lon"]


# 1) เตรียมข้อมูลสถานี -------------------------------------------------------
def province_from_area(area_th, station_type):
    """แยกจังหวัดจาก areaTH: สถานี กทม. ไม่มีชื่อจังหวัด, บางสถานีเขียน 'จ.' นำหน้า"""
    last_part = F.trim(F.element_at(F.split(area_th, ","), -1))
    cleaned = F.regexp_replace(last_part, r"^จ\.\s*", "")
    return (F.when(station_type == "BKK", F.lit("กรุงเทพฯ"))
             .when(cleaned == "กรุงเทพมหานคร", F.lit("กรุงเทพฯ"))
             .otherwise(cleaned))


def prepare_stations(df):
    """Bronze -> ข้อมูลสถานีล่าสุด 1 แถวต่อสถานี + ลายนิ้วมือ (attr_hash)"""
    stations = (
        df.select(
            F.col("stationID").alias("station_id"),
            F.col("nameTH").alias("name_th"),
            F.col("nameEN").alias("name_en"),
            F.col("areaTH").alias("area_th"),
            F.col("areaEN").alias("area_en"),
            province_from_area(F.col("areaTH"), F.col("stationType")).alias("province_th"),
            F.col("stationType").alias("station_type"),
            F.col("lat").cast("double").alias("lat"),
            F.col("long").cast("double").alias("lon"),
            F.regexp_extract("_source_file", r"(\d{8}_\d{6})", 1).alias("fetched_str"),
        )
        .withColumn("valid_from", F.expr("to_timestamp_ntz(fetched_str, 'yyyyMMdd_HHmmss')"))
        .drop("fetched_str")
        .filter(F.col("station_id").isNotNull() & F.col("valid_from").isNotNull())
        .withColumn("attr_hash", F.sha2(F.to_json(F.struct(*TRACKED_COLS)), 256))
    )
    latest = Window.partitionBy("station_id").orderBy(F.col("valid_from").desc())
    return stations.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")


# 2) SCD Type 2 ด้วย MERGE (ท่า merge_key) -----------------------------------
def scd2_merge(updates, target_table):
    """ปิดเวอร์ชันเก่า (valid_to, is_current=false) แล้วเพิ่มเวอร์ชันใหม่ในคำสั่งเดียว"""
    updates.createOrReplaceTempView("station_updates")
    updates.sparkSession.sql(f"""
        MERGE INTO {target_table} AS t
        USING (
            -- ก) ทุกสถานี ใช้ station_id เป็น key: ไว้ปิดเวอร์ชันเก่า / เพิ่มสถานีใหม่
            SELECT station_id AS merge_key, * FROM station_updates
            UNION ALL
            -- ข) เฉพาะสถานีที่ข้อมูลเปลี่ยน ใช้ key = NULL: บังคับให้ INSERT เวอร์ชันใหม่
            SELECT NULL AS merge_key, s.*
            FROM station_updates s
            JOIN {target_table} d ON s.station_id = d.station_id AND d.is_current
            WHERE s.attr_hash <> d.attr_hash AND s.valid_from > d.valid_from
        ) AS u
        ON t.station_id = u.merge_key AND t.is_current
        WHEN MATCHED AND t.attr_hash <> u.attr_hash AND u.valid_from > t.valid_from THEN
            UPDATE SET t.valid_to = u.valid_from, t.is_current = false
        WHEN NOT MATCHED THEN
            INSERT (station_id, name_th, name_en, area_th, area_en, province_th,
                    station_type, lat, lon, attr_hash, valid_from, valid_to, is_current)
            VALUES (u.station_id, u.name_th, u.name_en, u.area_th, u.area_en, u.province_th,
                    u.station_type, u.lat, u.lon, u.attr_hash, u.valid_from, NULL, true)
    """)


def upsert_dim_station(batch_df, batch_id):
    scd2_merge(prepare_stations(batch_df), DIM_TABLE)


# 3) สร้างตาราง + รัน --------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_TABLE} (
  station_sk    BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'surrogate key: 1 เลข = 1 เวอร์ชัน',
  station_id    STRING NOT NULL,
  name_th       STRING,
  name_en       STRING,
  area_th       STRING,
  area_en       STRING,
  province_th   STRING,
  station_type  STRING COMMENT 'GROUND / BKK / MOBILE',
  lat           DOUBLE,
  lon           DOUBLE,
  attr_hash     STRING,
  valid_from    TIMESTAMP_NTZ NOT NULL COMMENT 'เริ่มใช้ข้อมูลเวอร์ชันนี้ (เวลาไทย)',
  valid_to      TIMESTAMP_NTZ COMMENT 'NULL = ยังใช้อยู่',
  is_current    BOOLEAN NOT NULL
)
""")

query = (
    spark.readStream.table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(upsert_dim_station)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print("เสร็จแล้ว เขียนลง", DIM_TABLE)

# COMMAND ----------

from pyspark.sql import functions as F

dim = spark.table(DIM_TABLE)
print("แถวทั้งหมด:", dim.count(), "| เวอร์ชันปัจจุบัน:", dim.filter("is_current").count())
print("สถานีที่มีหลายเวอร์ชัน:", dim.groupBy("station_id").count().filter("count > 1").count())

# จำนวนสถานีต่อจังหวัด
display(dim.filter("is_current").groupBy("province_th").count().orderBy(F.desc("count")))

# รถตรวจวัดเคลื่อนที่ 8 คัน
display(dim.filter("station_type = 'MOBILE'")
        .select("station_sk", "station_id", "name_th", "province_th", "lat", "lon",
                "valid_from", "valid_to", "is_current"))
