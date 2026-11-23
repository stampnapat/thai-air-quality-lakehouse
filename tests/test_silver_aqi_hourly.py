from datetime import datetime

from notebook_loader import load_definitions

nb = load_definitions("04_silver_aqi_hourly")

BRONZE_SCHEMA = """
    stationID STRING,
    AQILast STRUCT<
        date: STRING, time: STRING,
        PM25: STRUCT<value: STRING, aqi: STRING>,
        PM10: STRUCT<value: STRING>, O3: STRUCT<value: STRING>,
        CO: STRUCT<value: STRING>, NO2: STRUCT<value: STRING>,
        SO2: STRUCT<value: STRING>,
        AQI: STRUCT<aqi: STRING, param: STRING>>,
    _source_file STRING
"""


def bronze_row(station, date, time, pm25, fetched, pm10="-1"):
    src = f"/Volumes/air_quality/raw/landing/air4thai/ingest_date=x/air4thai_{fetched}.json"
    aqi_last = (date, time, (pm25, "40"), (pm10,), ("-1",), ("-1",), ("-1",), ("-1",),
                ("40", "PM25"))
    return (station, aqi_last, src)


def clean(spark, rows):
    return nb["clean_bronze"](spark.createDataFrame(rows, BRONZE_SCHEMA))


def test_clean_bronze_parses_types_and_nulls_missing_values(spark):
    row = clean(spark, [bronze_row("02t", "2026-11-23", "14:00", "35.6", "20261123_141500")]).first()
    assert row.station_id == "02t"
    assert row.measured_at == datetime(2026, 11, 23, 14, 0)
    assert row.pm25 == 35.6
    assert row.pm10 is None  # Air4Thai sends -1 for "no data"
    assert row.fetched_at == datetime(2026, 11, 23, 14, 15)
    assert row.fetch_lag_minutes == 15
    assert row.is_stale is False


def test_clean_bronze_flags_stale_station(spark):
    # Reading is from weeks ago but was fetched today -> the station is stuck.
    row = clean(spark, [bronze_row("99t", "2026-10-09", "08:00", "20", "20261123_141500")]).first()
    assert row.is_stale is True


def test_clean_bronze_drops_rows_without_station(spark):
    assert clean(spark, [bronze_row(None, "2026-11-23", "14:00", "20", "20261123_141500")]).count() == 0


def test_dedup_keeps_first_fetch_per_station_hour(spark):
    rows = [
        bronze_row("02t", "2026-11-23", "14:00", "35.6", "20261123_141500"),
        bronze_row("02t", "2026-11-23", "14:00", "35.6", "20261123_151500"),  # same reading again
        bronze_row("02t", "2026-11-23", "15:00", "30.1", "20261123_151500"),
    ]
    out = nb["dedup"](clean(spark, rows))
    got = sorted((r.measured_at.hour, r.fetched_at.hour) for r in out.collect())
    assert got == [(14, 14), (15, 15)]


def test_row_hash_changes_only_when_values_change(spark):
    rows = [
        bronze_row("a", "2026-11-23", "14:00", "35.6", "20261123_141500"),
        bronze_row("b", "2026-11-23", "14:00", "35.6", "20261123_151500"),
        bronze_row("c", "2026-11-23", "14:00", "36.0", "20261123_141500"),
    ]
    h = {r.station_id: r.row_hash for r in clean(spark, rows).collect()}
    assert h["a"] == h["b"] != h["c"]
