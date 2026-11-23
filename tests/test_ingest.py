import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from notebook_loader import load_definitions

nb = load_definitions("01_ingest_air4thai")


def test_validate_returns_station_count():
    payload = json.dumps({"stations": [{"stationID": "02t"}, {"stationID": "03t"}]})
    assert nb["validate"](payload) == 2


@pytest.mark.parametrize("payload", ['{"stations": []}', '{"other": 1}'])
def test_validate_rejects_missing_or_empty_stations(payload):
    with pytest.raises(ValueError):
        nb["validate"](payload)


def test_validate_rejects_broken_json():
    with pytest.raises(ValueError):  # json.JSONDecodeError is a ValueError
        nb["validate"]("<html>maintenance</html>")


def test_save_raw_writes_partitioned_file_in_thai_time(tmp_path):
    ns = load_definitions("01_ingest_air4thai", LANDING_DIR=str(tmp_path))
    now = datetime(2026, 11, 23, 14, 15, 0, tzinfo=timezone(timedelta(hours=7)))
    path = Path(ns["save_raw"]('{"stations": [1]}', now))
    assert path.parent.name == "ingest_date=2026-11-23"
    assert path.name == "air4thai_20261123_141500.json"
    assert path.read_text(encoding="utf-8") == '{"stations": [1]}'


def test_intermediate_fingerprints_are_sha256():
    for url, fp in nb["INTERMEDIATES"].items():
        assert url.startswith("http://") and url.endswith(".lencr.org/")
        assert len(fp) == 64 and all(c in "0123456789abcdef" for c in fp)
