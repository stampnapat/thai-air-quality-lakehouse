# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "air_quality"
SOURCE_DIR = f"/Volumes/{CATALOG}/raw/landing/air4thai"
TARGET_TABLE = f"{CATALOG}.bronze.air4thai_aqi"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/air4thai_aqi"

# 1) Auto Loader อ่านไฟล์ JSON ใหม่จาก Volume
raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")                              # 1 ไฟล์ = JSON ก้อนเดียวหลายบรรทัด
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/_schema")
    .option("cloudFiles.inferColumnTypes", "true")           # อ่านเป็นข้อความทั้งหมดก่อน
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.partitionColumns", "ingest_date")     # ดึงวันที่จากชื่อโฟลเดอร์
    .load(SOURCE_DIR)
)

# 2) แตก 180 สถานีในไฟล์เป็น 180 แถว + เพิ่มคอลัมน์ควบคุม
bronze = (
    raw_stream
    .select(
        F.explode("stations").alias("station"),
        "ingest_date",
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
    )
    .select("station.*", "ingest_date", "_rescued_data", "_source_file", "_file_modified_at")
    .withColumn("_ingested_at", F.current_timestamp())
)

# 3) เขียนลงตาราง Bronze อ่านไฟล์ใหม่ให้หมดแล้วหยุด
query = (
    bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()
print("เสร็จแล้ว เขียนลง", TARGET_TABLE)




# COMMAND ----------


df = spark.table("air_quality.bronze.air4thai_aqi")
print("จำนวนแถวทั้งหมด:", df.count())

# แต่ละไฟล์ได้กี่แถว (ควรได้ไฟล์ละประมาณ 180)
display(df.groupBy("_source_file").count().orderBy("_source_file"))

# ตัวอย่างข้อมูล
display(df.select("stationID", "nameTH", "AQILast.date", "AQILast.time",
                  "AQILast.PM25.value", "_ingested_at").limit(10))