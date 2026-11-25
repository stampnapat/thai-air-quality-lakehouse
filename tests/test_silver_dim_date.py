from datetime import date

import pytest

from notebook_loader import load_definitions

nb = load_definitions("08_silver_dim_date")


@pytest.mark.parametrize("d, season", [
    (date(2026, 2, 15), "หนาว"), (date(2026, 2, 16), "ร้อน"),   # TMD boundaries
    (date(2026, 5, 15), "ร้อน"), (date(2026, 5, 16), "ฝน"),
    (date(2026, 10, 15), "ฝน"), (date(2026, 10, 16), "หนาว"),
    (date(2026, 12, 31), "หนาว"), (date(2026, 1, 1), "หนาว"),
])
def test_thai_season_boundaries(d, season):
    assert nb["thai_season"](d) == season


def test_winter_belongs_to_the_year_it_ends():
    assert nb["season_year"](date(2025, 11, 1)) == 2026
    assert nb["season_year"](date(2026, 2, 1)) == 2026
    assert nb["season_year"](date(2026, 10, 15)) == 2026


def test_thai_fiscal_year_starts_in_october():
    assert nb["fiscal_year_th"](date(2025, 9, 30)) == 2025
    assert nb["fiscal_year_th"](date(2025, 10, 1)) == 2026


def test_holidays_include_lunar_and_substitution_days():
    hol = nb["thai_holidays"]("2026-01-01", "2026-12-31")
    assert "สงกรานต์" in hol[date(2026, 4, 13)]
    assert hol[date(2026, 3, 3)] == "วันมาฆบูชา"          # lunar calendar, moves every year
    assert "ชดเชย" in hol[date(2026, 6, 1)]                # Visakha Bucha fell on a Sunday


@pytest.fixture(scope="module")
def dim(spark):
    rows = nb["build_dim_date"](spark, "2025-12-25", "2026-05-20").collect()
    return {r.date: r for r in rows}


def test_one_row_per_day_with_unique_keys(dim):
    assert len(dim) == (date(2026, 5, 20) - date(2025, 12, 25)).days + 1
    assert len({r.date_key for r in dim.values()}) == len(dim)
    assert dim[date(2026, 1, 1)].date_key == 20260101


def test_calendar_attributes(dim):
    r = dim[date(2026, 4, 13)]  # Monday, Songkran
    assert (r.year, r.year_be, r.quarter, r.month, r.month_name_th) == (2026, 2569, 2, 4, "เมษายน")
    assert (r.day_of_week, r.day_name_th, r.is_weekend) == (1, "จันทร์", False)
    assert r.is_holiday and not r.is_working_day and "สงกรานต์" in r.holiday_name_th
    assert r.season_th == "ร้อน" and r.is_haze_season


def test_working_day_and_haze_flags(dim):
    assert dim[date(2026, 4, 18)].is_weekend and not dim[date(2026, 4, 18)].is_working_day
    assert dim[date(2026, 4, 20)].is_working_day
    assert dim[date(2026, 4, 30)].is_haze_season and not dim[date(2026, 5, 1)].is_haze_season
    assert dim[date(2025, 12, 31)].holiday_name_th == "วันสิ้นปี"
