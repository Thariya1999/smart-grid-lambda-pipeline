"""
Unit tests for the simulated data sources (Project Requirement #1) and their
underlying generation logic -- correctness & robustness of the simulators.
"""
import random
from datetime import datetime, timezone

from sources.common import HOUSEHOLDS, GRID_ZONES, household_by_id, sim_day_index, sim_date_str
from sources.simulation_logic import (
    generate_meter_reading,
    generate_tariff_row,
    generate_weather_row,
    solar_irradiance_factor,
)


class TestHouseholdWorld:
    def test_households_nonempty_and_unique_ids(self):
        assert len(HOUSEHOLDS) > 0
        ids = [h.household_id for h in HOUSEHOLDS]
        assert len(ids) == len(set(ids))

    def test_every_household_in_known_zone(self):
        for h in HOUSEHOLDS:
            assert h.grid_zone in GRID_ZONES

    def test_household_lookup(self):
        first = HOUSEHOLDS[0]
        assert household_by_id(first.household_id) == first
        assert household_by_id("NOT-A-REAL-ID") is None


class TestMeterReadingGeneration:
    def test_reading_has_required_fields(self):
        h = HOUSEHOLDS[0]
        rng = random.Random(1)
        event = generate_meter_reading(h, now=datetime.now(timezone.utc), rng=rng)
        for field in ("meter_id", "household_id", "power_consumption_kwh",
                      "solar_generation_kwh", "grid_zone", "timestamp"):
            assert field in event

    def test_consumption_always_nonnegative(self):
        rng = random.Random(2)
        for h in HOUSEHOLDS:
            for _ in range(20):
                event = generate_meter_reading(h, rng=rng)
                assert event["power_consumption_kwh"] >= 0

    def test_non_solar_household_never_generates_solar(self):
        non_solar = next(h for h in HOUSEHOLDS if not h.has_solar)
        rng = random.Random(3)
        for _ in range(20):
            event = generate_meter_reading(non_solar, rng=rng)
            assert event["solar_generation_kwh"] == 0.0

    def test_solar_household_can_generate_solar(self):
        solar_hh = next(h for h in HOUSEHOLDS if h.has_solar)
        rng = random.Random(4)
        # sample a bunch of times; at least one should be > 0 given the sine model
        any_positive = False
        for i in range(50):
            from datetime import timedelta
            now = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i * 6)
            event = generate_meter_reading(solar_hh, now=now, rng=rng)
            if event["solar_generation_kwh"] > 0:
                any_positive = True
        assert any_positive

    def test_full_cloud_cover_dampens_solar(self):
        solar_hh = next(h for h in HOUSEHOLDS if h.has_solar)
        rng1, rng2 = random.Random(5), random.Random(5)
        noon = datetime(2026, 1, 1, 0, 2, 30, tzinfo=timezone.utc)  # mid-cycle -> peak sun
        clear = generate_meter_reading(solar_hh, now=noon, cloud_cover=0.0, rng=rng1)
        cloudy = generate_meter_reading(solar_hh, now=noon, cloud_cover=1.0, rng=rng2)
        assert cloudy["solar_generation_kwh"] <= clear["solar_generation_kwh"]


class TestSolarIrradianceFactor:
    def test_factor_in_bounds(self):
        for sec in range(0, 300, 15):
            now = datetime(2026, 1, 1, tzinfo=timezone.utc).replace(microsecond=0)
            from datetime import timedelta
            t = now + timedelta(seconds=sec)
            f = solar_irradiance_factor(t)
            assert 0.0 <= f <= 1.0


class TestBatchRowGeneration:
    def test_tariff_row_fields(self):
        rng = random.Random(6)
        row = generate_tariff_row(HOUSEHOLDS[0], rng=rng)
        assert set(row.keys()) == {"household_id", "tariff_rate", "billing_tier", "subsidy_flag"}
        assert row["tariff_rate"] > 0

    def test_subsidized_tier_flag_matches_household(self):
        rng = random.Random(6)
        subsidized_hh = next(h for h in HOUSEHOLDS if h.billing_tier == "RESIDENTIAL_SUBSIDIZED")
        row = generate_tariff_row(subsidized_hh, rng=rng)
        assert row["subsidy_flag"] is True

    def test_weather_row_fields(self):
        rng = random.Random(6)
        row = generate_weather_row("ZONE-A", "20260901", rng=rng)
        assert row["grid_zone"] == "ZONE-A"
        assert 0.0 <= row["cloud_cover"] <= 1.0
        assert row["forecast_summary"] in {"clear", "partly cloudy", "overcast"}


class TestSimulatedClock:
    def test_sim_day_index_progresses(self):
        from datetime import timedelta
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert sim_day_index(start, now=start) == 0
        assert sim_day_index(start, now=start + timedelta(seconds=300)) == 1
        assert sim_day_index(start, now=start + timedelta(seconds=650)) == 2

    def test_sim_date_str_increments_calendar_date(self):
        d0 = sim_date_str(0, start_date="2026-09-01")
        d1 = sim_date_str(1, start_date="2026-09-01")
        assert d0 == "20260901"
        assert d1 == "20260902"
