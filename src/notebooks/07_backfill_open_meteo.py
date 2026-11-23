# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill: PM2.5 ย้อนหลังจาก Open-Meteo (ค่าจากแบบจำลอง)
# MAGIC
# MAGIC Air4Thai ให้แค่ค่าล่าสุด ประวัติจึงเริ่มตั้งแต่วันที่เราเริ่มเก็บ notebook นี้เติมประวัติย้อนหลัง
# MAGIC จาก Open-Meteo Air Quality API (แบบจำลอง CAMS ความละเอียดประมาณ 0.4° หรือ ~45 กม.)
# MAGIC
# MAGIC **ข้อควรระวัง:** ค่านี้เป็น *ค่าประมาณจากแบบจำลอง* ไม่ใช่ค่าที่สถานีวัดได้จริง
# MAGIC จึงเก็บแยกตาราง (`pm25_model_hourly`) ไม่ปนกับข้อมูล Air4Thai
# MAGIC
# MAGIC รันครั้งแรกดึงตั้งแต่ FIRST_DATE จากนั้น job รันทุกวันเพื่อต่อข้อมูลเฉพาะวันล่าสุด
# MAGIC
# MAGIC Flow: สถานีปัจจุบันจาก `dim_station` → เรียก API ทีละ 50 สถานี → เก็บ JSON ดิบลง Volume
# MAGIC → MERGE ลง Silver → สร้าง Gold รายจังหวัด + ตารางเทียบค่าจริงกับแบบจำลอง

# COMMAND ----------

import json
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

dbutils.widgets.text("catalog", "air_quality")
dbutils.widgets.text("start_date", "")  # ว่าง = ต่อจากวันล่าสุดที่มี (ย้อน 2 วัน) หรือ FIRST_DATE ถ้ายังไม่มีตาราง
dbutils.widgets.text("end_date", "")  # ว่าง = เมื่อวาน (เวลาไทย)

CATALOG = dbutils.widgets.get("catalog")
BKK = timezone(timedelta(hours=7))
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
BATCH_SIZE = 50  # Open-Meteo รับหลายพิกัดในคำขอเดียว
RAW_DIR = f"/Volumes/{CATALOG}/raw/landing/open_meteo"
MODEL_TABLE = f"{CATALOG}.silver.pm25_model_hourly"
FIRST_DATE = "2025-11-01"  # จุดเริ่มประวัติย้อนหลังรอบแรก
OVERLAP_DAYS = 2           # ดึงซ้ำ 2 วันล่าสุดเผื่อแบบจำลองปรับค่า (MERGE กันซ้ำ)


def chunks(items, size):
    """แบ่ง list เป็นก้อนละ size ตัว"""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_batch(stations, start, end, attempts=3):
    """เรียก API ครั้งเดียวสำหรับหลายสถานี คืน list ของผลลัพธ์ เรียงตามลำดับพิกัดที่ส่งไป"""
    params = {
        "latitude": ",".join(f"{s['lat']:.4f}" for s in stations),
        "longitude": ",".join(f"{s['lon']:.4f}" for s in stations),
        "hourly": "pm2_5",
        "start_date": start,
        "end_date": end,
        "timezone": "Asia/Bangkok",
    }
    for i in range(1, attempts + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=120)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else [data]  # 1 พิกัด = คืนเป็น object
        except requests.RequestException:
            if i == attempts:
                raise
            time.sleep(10 * i)


def to_rows(stations, results):
    """แปลงผลลัพธ์ API เป็นแถว (station_id, measured_at, pm25)"""
    rows = []
    for st, res in zip(stations, results):
        hourly = res["hourly"]
        for t, v in zip(hourly["time"], hourly["pm2_5"]):
            if v is not None:
                rows.append((st["station_id"], t, float(v)))
    return rows


# COMMAND ----------

end_date = dbutils.widgets.get("end_date") or (datetime.now(BKK).date() - timedelta(days=1)).isoformat()
start_date = dbutils.widgets.get("start_date")
if not start_date:
    # รอบรายวัน: ดึงเฉพาะช่วงใหม่ แทนการดึงทั้งปีซ้ำ
    last = (spark.sql(f"SELECT MAX(measured_date) AS d FROM {MODEL_TABLE}").first().d
            if spark.catalog.tableExists(MODEL_TABLE) else None)
    start_date = (last - timedelta(days=OVERLAP_DAYS)).isoformat() if last else FIRST_DATE

stations = [r.asDict() for r in spark.sql(f"""
    SELECT station_id, lat, lon FROM {CATALOG}.silver.dim_station
    WHERE is_current AND lat IS NOT NULL AND lon IS NOT NULL
""").collect()]
print(f"สถานี {len(stations)} แห่ง ช่วง {start_date} ถึง {end_date}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")
run_dir = f"{RAW_DIR}/run={datetime.now(BKK):%Y%m%d_%H%M%S}"
dbutils.fs.mkdirs(run_dir)

all_rows = []
for n, batch in enumerate(chunks(stations, BATCH_SIZE), start=1):
    results = fetch_batch(batch, start_date, end_date)
    # Bronze: เก็บคำตอบดิบไว้ก่อนแปลง (ย้อนตรวจ/รันใหม่ได้)
    dbutils.fs.put(f"{run_dir}/batch_{n:02d}.json",
                   json.dumps({"stations": batch, "results": results}), overwrite=True)
    all_rows.extend(to_rows(batch, results))
    print(f"batch {n}: {len(batch)} สถานี")
    time.sleep(1)  # สุภาพกับ API ฟรี

print(f"ได้ {len(all_rows):,} แถว")

# COMMAND ----------

# Silver: MERGE ลงตาราง (รันซ้ำได้ ไม่ซ้ำ)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MODEL_TABLE} (
  station_id   STRING NOT NULL,
  measured_at  TIMESTAMP_NTZ NOT NULL COMMENT 'ชั่วโมง (เวลาไทย)',
  measured_date DATE,
  pm25_model   DOUBLE COMMENT 'ug/m3 จากแบบจำลอง CAMS ผ่าน Open-Meteo (ไม่ใช่ค่าวัดจริง)',
  source       STRING,
  _loaded_at   TIMESTAMP
)
CLUSTER BY (measured_date)
COMMENT 'PM2.5 รายชั่วโมงย้อนหลังจากแบบจำลอง ใช้เติมประวัติก่อนเริ่มเก็บ Air4Thai'
""")

pdf = pd.DataFrame(all_rows, columns=["station_id", "measured_at_str", "pm25_model"])
updates = (
    spark.createDataFrame(pdf)
    .selectExpr("station_id",
                "to_timestamp_ntz(measured_at_str, \"yyyy-MM-dd'T'HH:mm\") AS measured_at",
                "pm25_model")
    .selectExpr("*", "to_date(measured_at) AS measured_date",
                "'open-meteo/cams' AS source", "current_timestamp() AS _loaded_at")
)
updates.createOrReplaceTempView("model_updates")
spark.sql(f"""
MERGE INTO {MODEL_TABLE} t
USING model_updates s
ON t.station_id = s.station_id AND t.measured_at = s.measured_at
WHEN MATCHED AND t.pm25_model <> s.pm25_model THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("Silver:", spark.table(MODEL_TABLE).count(), "แถว")

# COMMAND ----------

# Gold: รายจังหวัดรายวัน (แบบจำลอง) + ตารางเทียบกับค่าที่วัดจริง
spark.sql(f"USE CATALOG {CATALOG}")

spark.sql("""
CREATE OR REPLACE TABLE gold.pm25_model_province_daily
CLUSTER BY (measured_date, province_th)
COMMENT 'PM2.5 รายวันต่อจังหวัดจากแบบจำลอง Open-Meteo (ประวัติย้อนหลัง)'
AS
SELECT d.province_th, m.measured_date,
       ROUND(AVG(m.pm25_model), 1)    AS pm25_model_avg,
       ROUND(MAX(m.pm25_model), 1)    AS pm25_model_max_hourly,
       COUNT(DISTINCT m.station_id)   AS n_stations,
       AVG(m.pm25_model) > 37.5       AS exceeds_standard
FROM silver.pm25_model_hourly m
JOIN silver.dim_station d ON m.station_id = d.station_id AND d.is_current
GROUP BY ALL
""")

spark.sql("""
CREATE OR REPLACE TABLE gold.pm25_measured_vs_model
COMMENT 'เทียบค่าเฉลี่ยรายวันที่สถานีวัดได้ (Air4Thai) กับแบบจำลอง ในวันที่มีทั้งสองแหล่ง'
AS
SELECT s.station_id, s.name_th, s.province_th, s.measured_date,
       s.pm25_avg                          AS pm25_measured,
       ROUND(AVG(m.pm25_model), 1)         AS pm25_model,
       ROUND(AVG(m.pm25_model) - s.pm25_avg, 1) AS model_minus_measured
FROM gold.pm25_station_daily s
JOIN silver.pm25_model_hourly m
  ON m.station_id = s.station_id AND m.measured_date = s.measured_date
WHERE s.is_complete_day
GROUP BY ALL
""")

display(spark.sql("""
SELECT COUNT(*) AS station_days,
       ROUND(AVG(model_minus_measured), 1)        AS mean_bias,
       ROUND(AVG(ABS(model_minus_measured)), 1)   AS mean_abs_error,
       ROUND(CORR(pm25_measured, pm25_model), 2)  AS correlation
FROM gold.pm25_measured_vs_model
"""))
