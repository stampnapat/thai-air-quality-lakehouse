import pytest
from pyspark.sql import functions as F

from notebook_loader import load_definitions

nb = load_definitions("05_silver_dim_station")

STATION_SCHEMA = ("stationID STRING, nameTH STRING, nameEN STRING, areaTH STRING, areaEN STRING, "
                  "stationType STRING, lat STRING, long STRING, _source_file STRING")


@pytest.mark.parametrize("area_th, station_type, expected", [
    ("สวนลุมพินี เขตปทุมวัน", "BKK", "กรุงเทพฯ"),  # Bangkok stations have no province
    ("ต.ช้างเผือก อ.เมือง, เชียงใหม่", "GROUND", "เชียงใหม่"),
    ("ต.บ้านเหนือ อ.เมือง, จ.กาญจนบุรี", "GROUND", "กาญจนบุรี"),  # strip the "จ." prefix
    ("แขวงดินแดง เขตดินแดง, กรุงเทพมหานคร", "GROUND", "กรุงเทพฯ"),
])
def test_province_from_area(spark, area_th, station_type, expected):
    df = spark.createDataFrame([(area_th, station_type)], "area STRING, type STRING")
    got = df.select(nb["province_from_area"](F.col("area"), F.col("type")).alias("p")).first().p
    assert got == expected


def station(sid, lat, fetched):
    return (sid, "ชื่อ", "Name", "ต.ก อ.ข, ลำปาง", "Lampang", "MOBILE", lat, "99.5",
            f"/Volumes/x/air4thai_{fetched}.json")


def prepare(spark, rows):
    return nb["prepare_stations"](spark.createDataFrame(rows, STATION_SCHEMA))


def test_prepare_stations_keeps_latest_version_per_station(spark):
    rows = [station("o64", "18.1", "20261123_141500"), station("o64", "18.9", "20261125_091500")]
    out = prepare(spark, rows).collect()
    assert len(out) == 1
    assert out[0].lat == 18.9 and out[0].province_th == "ลำปาง"


def test_attr_hash_changes_when_station_moves(spark):
    rows = [station("a", "18.1", "20261123_141500"), station("b", "18.9", "20261123_141500"),
            station("c", "18.1", "20261124_141500")]
    h = {r.station_id: r.attr_hash for r in prepare(spark, rows).collect()}
    assert h["a"] == h["c"]  # same attributes, different fetch time -> same hash
    assert h["a"] != h["b"]  # the station moved -> new SCD2 version
