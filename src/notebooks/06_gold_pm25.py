# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE air_quality.gold.pm25_station_daily
# MAGIC CLUSTER BY (measured_date, province_th)
# MAGIC COMMENT 'ค่า PM2.5 รายวันต่อสถานี (เฉลี่ยจากค่ารายชั่วโมง)'
# MAGIC AS
# MAGIC WITH dim AS (
# MAGIC   -- เวอร์ชันแรกของแต่ละสถานี ให้ถือว่าใช้มาตั้งแต่ต้น
# MAGIC   -- (เพราะเราเริ่มเก็บประวัติสถานีทีหลังค่าที่วัดชุดแรก)
# MAGIC   SELECT *,
# MAGIC          CASE WHEN valid_from = MIN(valid_from) OVER (PARTITION BY station_id)
# MAGIC               THEN TIMESTAMP_NTZ '1900-01-01 00:00:00'
# MAGIC               ELSE valid_from END AS effective_from
# MAGIC   FROM air_quality.silver.dim_station
# MAGIC ),
# MAGIC hourly AS (
# MAGIC   -- จับคู่ค่าที่วัด กับข้อมูลสถานี "เวอร์ชันที่ใช้อยู่ ณ เวลาที่วัด" (as-of join)
# MAGIC   SELECT s.station_id, d.station_sk, d.name_th, d.province_th, d.station_type,
# MAGIC          d.lat, d.lon, s.measured_date, s.pm25
# MAGIC   FROM air_quality.silver.aqi_hourly s
# MAGIC   JOIN dim d
# MAGIC     ON  s.station_id = d.station_id
# MAGIC     AND s.measured_at >= d.effective_from
# MAGIC     AND (d.valid_to IS NULL OR s.measured_at < d.valid_to)
# MAGIC   WHERE s.pm25 IS NOT NULL
# MAGIC     AND NOT s.is_stale
# MAGIC )
# MAGIC SELECT station_id, station_sk, name_th, province_th, station_type, lat, lon, measured_date,
# MAGIC        ROUND(AVG(pm25), 1) AS pm25_avg,
# MAGIC        MAX(pm25)           AS pm25_max,
# MAGIC        COUNT(*)            AS hours_with_data,
# MAGIC        COUNT(*) >= 18      AS is_complete_day,
# MAGIC        AVG(pm25) > 37.5    AS exceeds_standard
# MAGIC FROM hourly
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE air_quality.gold.pm25_province_daily
# MAGIC CLUSTER BY (measured_date, province_th)
# MAGIC COMMENT 'ค่า PM2.5 รายวันต่อจังหวัด'
# MAGIC AS
# MAGIC SELECT province_th, measured_date,
# MAGIC        ROUND(AVG(pm25_avg), 1)     AS pm25_avg,
# MAGIC        MAX(pm25_max)               AS pm25_max_hourly,
# MAGIC        COUNT(*)                    AS n_stations,
# MAGIC        COUNT_IF(exceeds_standard)  AS n_stations_exceed,
# MAGIC        MAX_BY(name_th, pm25_avg)   AS worst_station,
# MAGIC        BOOL_AND(is_complete_day)   AS is_complete_day
# MAGIC FROM air_quality.gold.pm25_station_daily
# MAGIC GROUP BY province_th, measured_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE air_quality.gold.pm25_latest
# MAGIC COMMENT 'ค่า PM2.5 ล่าสุดของแต่ละสถานี สำหรับแผนที่'
# MAGIC AS
# MAGIC SELECT s.station_id, d.name_th, d.province_th, d.station_type, d.lat, d.lon,
# MAGIC        s.measured_at, s.pm25,
# MAGIC        CASE WHEN s.pm25 <= 15   THEN '1 ดีมาก'
# MAGIC             WHEN s.pm25 <= 25   THEN '2 ดี'
# MAGIC             WHEN s.pm25 <= 37.5 THEN '3 ปานกลาง'
# MAGIC             WHEN s.pm25 <= 75   THEN '4 เริ่มมีผลกระทบต่อสุขภาพ'
# MAGIC             ELSE                     '5 มีผลกระทบต่อสุขภาพ'
# MAGIC        END AS pm25_level
# MAGIC FROM air_quality.silver.aqi_hourly s
# MAGIC JOIN air_quality.silver.dim_station d
# MAGIC   ON s.station_id = d.station_id AND d.is_current
# MAGIC WHERE s.pm25 IS NOT NULL
# MAGIC   AND NOT s.is_stale
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY s.station_id ORDER BY s.measured_at DESC) = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT province_th, measured_date, pm25_avg, n_stations,
# MAGIC        n_stations_exceed, worst_station, is_complete_day
# MAGIC FROM air_quality.gold.pm25_province_daily
# MAGIC WHERE measured_date = (SELECT MAX(measured_date) FROM air_quality.gold.pm25_province_daily)
# MAGIC ORDER BY pm25_avg DESC
# MAGIC LIMIT 10