# Databricks notebook source
import hashlib, ssl, certifi, requests, tempfile, os

# ใบรับรองตัวกลางของ Let's Encrypt ที่เซิร์ฟเวอร์ Air4Thai ไม่ได้ส่งมา
# ตรวจลายนิ้วมือ (SHA-256) ก่อนใช้ เพื่อกันใบรับรองปลอม
INTERMEDIATES = {
    "http://yr1.i.lencr.org/": "13949634d99cd6fd6aa80bc034fefacceb1969feef986586713ecdbb05758d3f",
    "http://yr.i.lencr.org/":  "072639d0b140d5bffae16ad9c3f6cc6086040621f51ee61a6d46a8915c07cf76",
}

pems = []
for cert_url, expected in INTERMEDIATES.items():
    der = requests.get(cert_url, timeout=30).content
    actual = hashlib.sha256(der).hexdigest()
    if actual != expected:
        raise ValueError(f"ลายนิ้วมือไม่ตรง: {cert_url}")
    pems.append(ssl.DER_cert_to_PEM_cert(der))

bundle_path = os.path.join(tempfile.gettempdir(), "air4thai_ca_bundle.pem")
with open(certifi.where()) as f:
    base = f.read()
with open(bundle_path, "w") as f:
    f.write(base + "\n" + "\n".join(pems))

url = "https://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
r = requests.get(url, timeout=30, verify=bundle_path)
data = r.json()
print("สถานะ:", r.status_code)
print("จำนวนสถานี:", len(data["stations"]))
print("ตัวอย่าง:", data["stations"][0]["nameTH"], data["stations"][0]["AQILast"]["PM25"])


# COMMAND ----------

CATALOG = "air_quality"

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print("สร้าง catalog ไม่ได้ จะใช้ workspace แทน:", str(e)[:200])
    CATALOG = "workspace"

for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

test_path = f"/Volumes/{CATALOG}/raw/landing/_test.txt"
dbutils.fs.put(test_path, "hello air quality", overwrite=True)
print("เขียนไฟล์ได้:", dbutils.fs.head(test_path))
dbutils.fs.rm(test_path)

print("ใช้ catalog:", CATALOG)
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))