# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: dim_date (มิติเวลา)
# MAGIC
# MAGIC 1 แถว = 1 วัน ตั้งแต่ DATE_START ถึงสิ้นปีถัดไป ใช้ join กับ fact ด้วย `date_key` (yyyymmdd)
# MAGIC หรือ `date` เพื่อให้ทุก query และ dashboard ใช้นิยามเดียวกันของ
# MAGIC ฤดูกาล, ฤดูหมอกควัน, วันหยุดราชการ, ปีงบประมาณ และ พ.ศ.
# MAGIC
# MAGIC - **ฤดูกาล** ตามกรมอุตุนิยมวิทยา: ร้อน 16 ก.พ.–15 พ.ค., ฝน 16 พ.ค.–15 ต.ค., หนาว 16 ต.ค.–15 ก.พ.
# MAGIC - **ฤดูหมอกควัน** (`is_haze_season`): 1 ม.ค.–30 เม.ย. ช่วงที่ภาคเหนือเผาและค่าฝุ่นสูงสุด
# MAGIC   (ข้อมูลแบบจำลองในโปรเจกต์นี้: เชียงใหม่สูงสุดเดือนเมษายน)
# MAGIC - **วันหยุดราชการ** จากไลบรารี `holidays` (MIT) ซึ่งรวมวันพระตามปฏิทินจันทรคติ วันหยุดชดเชย
# MAGIC   และวันหยุดพิเศษที่ ครม. ประกาศ ไม่ต้องพิมพ์วันหยุดเองทุกปี
# MAGIC
# MAGIC ตารางเล็ก (ประมาณ 800 แถว) และคำนวณได้จากสูตรล้วน จึงสร้างใหม่ทั้งตารางทุกครั้ง (idempotent)

# COMMAND ----------

# MAGIC %pip install holidays==0.109

# COMMAND ----------

from datetime import date

import holidays
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "air_quality")  # job parameter; dev target uses air_quality_dev
CATALOG = dbutils.widgets.get("catalog")
DIM_TABLE = f"{CATALOG}.silver.dim_date"
DATE_START = "2025-01-01"  # ก่อนข้อมูลแบบจำลองชุดแรก (2025-11-01)
HAZE_MONTHS = (1, 2, 3, 4)  # ม.ค.–เม.ย.
MONTHS_TH = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", "กรกฎาคม",
             "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
DAYS_TH = ["จันทร์", "อังคาร", "พุธ", "พฤหัสบดี", "ศุกร์", "เสาร์", "อาทิตย์"]


def date_end(today: date) -> str:
    """สิ้นปีถัดไป: dashboard ที่มองไปข้างหน้ายัง join ได้"""
    return f"{today.year + 1}-12-31"


def thai_season(d: date) -> str:
    """ฤดูกาลตามกรมอุตุนิยมวิทยา (ขอบเขตวันที่ 16 ของ ก.พ. / พ.ค. / ต.ค.)"""
    md = (d.month, d.day)
    if (2, 16) <= md <= (5, 15):
        return "ร้อน"
    if (5, 16) <= md <= (10, 15):
        return "ฝน"
    return "หนาว"


def season_year(d: date) -> int:
    """ฤดูหนาวคาบเกี่ยวสองปี (ต.ค.–ก.พ.): นับเป็นปีที่ฤดูนั้นจบ เช่น หนาว ต.ค. 2025–ก.พ. 2026 = 2026"""
    return d.year + 1 if (d.month, d.day) >= (10, 16) else d.year


def fiscal_year_th(d: date) -> int:
    """ปีงบประมาณไทย (ค.ศ.) เริ่ม 1 ต.ค.: ต.ค. 2025 อยู่ในปีงบ 2026"""
    return d.year + 1 if d.month >= 10 else d.year


def thai_holidays(start: str, end: str) -> dict:
    """{date: ชื่อวันหยุด} วันหยุดราชการ (public) รวมวันชดเชยและวันหยุดพิเศษ"""
    years = range(int(start[:4]), int(end[:4]) + 1)
    return dict(holidays.Thailand(years=years, language="th"))


def build_dim_date(spark, start: str, end: str):
    """DataFrame 1 แถวต่อวัน; ค่าที่ขึ้นกับปฏิทินไทยคำนวณใน Python แล้วส่งให้ Spark"""
    hol = thai_holidays(start, end)
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS date").collect()
    rows = []
    for r in days:
        d = r.date
        rows.append((
            d,
            int(d.strftime("%Y%m%d")),
            d.year, d.year + 543, (d.month - 1) // 3 + 1, d.month, MONTHS_TH[d.month - 1],
            int(d.strftime("%V")), d.day, d.isoweekday(), DAYS_TH[d.weekday()],
            d.weekday() >= 5, d in hol, hol.get(d),
            thai_season(d), season_year(d), d.month in HAZE_MONTHS, fiscal_year_th(d),
        ))
    schema = ("date DATE, date_key INT, year INT, year_be INT, quarter INT, month INT, "
              "month_name_th STRING, iso_week INT, day_of_month INT, day_of_week INT, "
              "day_name_th STRING, is_weekend BOOLEAN, is_holiday BOOLEAN, holiday_name_th STRING, "
              "season_th STRING, season_year INT, is_haze_season BOOLEAN, fiscal_year_th INT")
    return (spark.createDataFrame(rows, schema)
            .withColumn("is_working_day", ~F.col("is_weekend") & ~F.col("is_holiday")))

# COMMAND ----------

end = date_end(date.today())
dim = build_dim_date(spark, DATE_START, end)
(dim.write.mode("overwrite").option("overwriteSchema", "true")
    .option("comment", "มิติเวลา: 1 แถวต่อวัน พร้อมฤดูกาล ฤดูหมอกควัน วันหยุดราชการไทย ปีงบประมาณ")
    .saveAsTable(DIM_TABLE))
spark.sql(f"ALTER TABLE {DIM_TABLE} ALTER COLUMN date_key SET NOT NULL")
print(f"{DIM_TABLE}: {DATE_START} ถึง {end}")

# COMMAND ----------

# ตรวจ: วันต้องต่อเนื่อง ไม่ซ้ำ และมีวันหยุดทุกปี
display(spark.sql(f"""
SELECT COUNT(*) AS days, COUNT(DISTINCT date_key) AS distinct_keys,
       DATEDIFF(MAX(date), MIN(date)) + 1 AS expected_days,
       COUNT_IF(is_holiday) AS holidays, COUNT_IF(is_haze_season) AS haze_days
FROM {DIM_TABLE}
"""))
