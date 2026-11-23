# Databricks notebook source
# MAGIC %sql
# MAGIC -- สถานีเดียวกัน วันเวลาเดียวกัน แต่โผล่หลายไฟล์
# MAGIC SELECT stationID, nameTH,
# MAGIC        AQILast.date AS date, AQILast.time AS time,
# MAGIC        COUNT(*) AS n_copies
# MAGIC FROM air_quality.bronze.air4thai_aqi
# MAGIC GROUP BY ALL
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY n_copies DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- สถานีที่ค่าล่าสุดไม่ใช่ของวันนี้
# MAGIC SELECT stationID, nameTH, stationType,
# MAGIC        MAX(AQILast.date) AS last_date
# MAGIC FROM air_quality.bronze.air4thai_aqi
# MAGIC GROUP BY stationID, nameTH, stationType
# MAGIC HAVING MAX(AQILast.date) < CAST(current_date() AS STRING)
# MAGIC ORDER BY last_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- เปอร์เซ็นต์ค่าที่หายไปของแต่ละสาร
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   ROUND(AVG(CASE WHEN AQILast.PM25.value = '-1' THEN 1 ELSE 0 END) * 100, 1) AS pm25_missing_pct,
# MAGIC   ROUND(AVG(CASE WHEN AQILast.PM10.value = '-1' THEN 1 ELSE 0 END) * 100, 1) AS pm10_missing_pct,
# MAGIC   ROUND(AVG(CASE WHEN AQILast.O3.value   = '-1' THEN 1 ELSE 0 END) * 100, 1) AS o3_missing_pct,
# MAGIC   ROUND(AVG(CASE WHEN AQILast.CO.value   = '-1' THEN 1 ELSE 0 END) * 100, 1) AS co_missing_pct
# MAGIC FROM air_quality.bronze.air4thai_aqi
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- เอาคำหลังเครื่องหมายจุลภาค (,) ตัวสุดท้ายมาเป็นจังหวัด แล้วดูว่าได้ผลแปลกๆ ไหม
# MAGIC SELECT stationType,
# MAGIC        trim(element_at(split(areaTH, ','), -1)) AS province_guess,
# MAGIC        COUNT(DISTINCT stationID) AS n_stations
# MAGIC FROM air_quality.bronze.air4thai_aqi
# MAGIC GROUP BY ALL
# MAGIC ORDER BY stationType, province_guess