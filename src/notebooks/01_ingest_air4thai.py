# Databricks notebook source
import hashlib, json, os, ssl, tempfile, time
from datetime import datetime, timedelta, timezone

import certifi
import requests

dbutils.widgets.text("catalog", "air_quality")  # job parameter; dev target uses air_quality_dev
CATALOG = dbutils.widgets.get("catalog")
LANDING_DIR = f"/Volumes/{CATALOG}/raw/landing/air4thai"
API_URL = "https://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
BKK = timezone(timedelta(hours=7))  # เวลาไทย

# ใบรับรองตัวกลางที่เซิร์ฟเวอร์ Air4Thai ไม่ได้ส่งมา (ดูขั้นที่ 1)
INTERMEDIATES = {
    "http://yr1.i.lencr.org/": "13949634d99cd6fd6aa80bc034fefacceb1969feef986586713ecdbb05758d3f",
    "http://yr.i.lencr.org/":  "072639d0b140d5bffae16ad9c3f6cc6086040621f51ee61a6d46a8915c07cf76",
}


def build_ca_bundle() -> str:
    """รวมใบรับรองมาตรฐานกับใบรับรองตัวกลางที่ขาด แล้วคืน path ของไฟล์"""
    pems = []
    for cert_url, expected in INTERMEDIATES.items():
        der = requests.get(cert_url, timeout=30).content
        if hashlib.sha256(der).hexdigest() != expected:
            raise ValueError(f"ลายนิ้วมือไม่ตรง: {cert_url}")
        pems.append(ssl.DER_cert_to_PEM_cert(der))
    path = os.path.join(tempfile.gettempdir(), "air4thai_ca_bundle.pem")
    with open(certifi.where()) as src, open(path, "w") as dst:
        dst.write(src.read() + "\n" + "\n".join(pems))
    return path


def fetch_with_retry(url: str, ca_bundle: str, attempts: int = 3) -> str:
    """ดึงข้อมูล ถ้าพังให้ลองใหม่ รอ 5, 10 วินาที"""
    for i in range(1, attempts + 1):
        try:
            r = requests.get(url, timeout=30, verify=ca_bundle)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if i == attempts:
                raise
            print(f"ครั้งที่ {i} พัง ({type(e).__name__}) ลองใหม่...")
            time.sleep(5 * i)


def validate(raw_text: str) -> int:
    """เช็คว่าข้อมูลหน้าตาถูกต้องก่อนเก็บ คืนจำนวนสถานี"""
    data = json.loads(raw_text)
    stations = data.get("stations")
    if not stations:
        raise ValueError("ไม่มี key 'stations' หรือว่างเปล่า")
    return len(stations)


def save_raw(raw_text: str, now: datetime) -> str:
    """เก็บไฟล์ดิบแบบไม่แก้อะไรเลย แยกโฟลเดอร์ตามวัน"""
    folder = f"{LANDING_DIR}/ingest_date={now:%Y-%m-%d}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/air4thai_{now:%Y%m%d_%H%M%S}.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(raw_text)
    return path


now = datetime.now(BKK)
raw_text = fetch_with_retry(API_URL, build_ca_bundle())
n_stations = validate(raw_text)
saved_path = save_raw(raw_text, now)

print("เวลาที่ดึง:", now.strftime("%Y-%m-%d %H:%M:%S"), "(เวลาไทย)")
print("จำนวนสถานี:", n_stations)
print("บันทึกไว้ที่:", saved_path)
print("ขนาดไฟล์:", round(os.path.getsize(saved_path) / 1024, 1), "KB")
