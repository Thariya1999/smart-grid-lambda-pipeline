"""
Pure, side-effect-free simulation & billing logic.

Kept separate from meter_simulator.py / tariff_batch_source.py (which do I/O:
Kafka, files, network) specifically so it can be unit-tested without a
broker, a filesystem, or Spark -- see tests/test_billing_logic.py and
tests/test_simulators.py.
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timezone

from sources.common import Household, TARIFF_RATE_BY_TIER


# --------------------------------------------------------------------------
# Streaming source: smart-meter reading generation
# --------------------------------------------------------------------------
def solar_irradiance_factor(now: datetime, cloud_cover: float = 0.0) -> float:
    """
    Model solar generation potential as a function of time-of-day and cloud
    cover, in [0, 1]. Peaks at local "noon" (modeled here as a sine wave over
    a compressed 24h-in-SIM_DAY_SECONDS cycle) and is zero at "night".

    `cloud_cover` in [0, 1] (0 = clear sky, 1 = fully overcast) linearly
    dampens the peak, matching the brief's note that a weather-forecast file
    can affect solar output.
    """
    # Use wall-clock seconds-of-minute as a stand-in "hour of compressed day"
    # so behaviour is deterministic and easy to reason about/tests.
    frac_of_cycle = (now.timestamp() % 300) / 300.0  # 0..1 across a 5-min cycle
    raw = math.sin(frac_of_cycle * math.pi)  # 0 at edges, 1 at midpoint ("noon")
    raw = max(0.0, raw)
    return raw * (1.0 - 0.8 * cloud_cover)


def generate_meter_reading(
    household: Household,
    now: datetime | None = None,
    cloud_cover: float = 0.0,
    rng: random.Random | None = None,
) -> dict:
    """Generate one simulated smart-meter reading event for a household."""
    now = now or datetime.now(timezone.utc)
    rng = rng or random

    # Base household consumption: residential ~0.1-1.2 kWh per short interval,
    # small businesses draw more.
    base_load = 0.6 if household.billing_tier == "SMALL_BUSINESS" else 0.35
    noise = rng.uniform(-0.15, 0.25)
    power_consumption_kwh = max(0.01, round(base_load + noise, 4))

    solar_generation_kwh = 0.0
    if household.has_solar:
        factor = solar_irradiance_factor(now, cloud_cover=cloud_cover)
        panel_capacity_kwh = 0.8  # per-interval max output of a rooftop system
        solar_generation_kwh = round(panel_capacity_kwh * factor * rng.uniform(0.85, 1.0), 4)

    return {
        "meter_id": household.meter_id,
        "household_id": household.household_id,
        "power_consumption_kwh": power_consumption_kwh,
        "solar_generation_kwh": solar_generation_kwh,
        "grid_zone": household.grid_zone,
        "timestamp": now.isoformat(),
    }


# --------------------------------------------------------------------------
# Daily-batch source: tariff / billing + weather forecast rows
# --------------------------------------------------------------------------
def generate_tariff_row(household: Household, rng: random.Random | None = None) -> dict:
    """One row of the daily tariff/billing reference file for a household."""
    rng = rng or random
    tariff_rate = TARIFF_RATE_BY_TIER[household.billing_tier]
    subsidy_flag = household.billing_tier == "RESIDENTIAL_SUBSIDIZED"
    # Small daily jitter on the rate to simulate time-of-use/demand pricing
    # adjustments a real tariff engine might apply.
    jitter = rng.uniform(-1.5, 1.5)
    return {
        "household_id": household.household_id,
        "tariff_rate": round(tariff_rate + jitter, 2),
        "billing_tier": household.billing_tier,
        "subsidy_flag": subsidy_flag,
    }


def generate_weather_row(zone: str, sim_date: str, rng: random.Random | None = None) -> dict:
    """One row of the daily weather-forecast file (affects solar output)."""
    rng = rng or random
    cloud_cover = round(rng.uniform(0.0, 0.9), 2)
    return {
        "grid_zone": zone,
        "date": sim_date,
        "cloud_cover": cloud_cover,
        "forecast_summary": "overcast" if cloud_cover > 0.6 else ("partly cloudy" if cloud_cover > 0.25 else "clear"),
    }


# --------------------------------------------------------------------------
# Billing computation -- the "meaningful transformation" the batch layer
# performs by joining streaming-derived daily consumption with the
# tariff/billing reference data. Pulled out here (pure function) so both
# spark/batch_job.py (Spark UDF-free, using these as plain arithmetic on
# columns) and the unit tests exercise identical logic.
# --------------------------------------------------------------------------
def compute_household_bill(
    total_consumption_kwh: float,
    total_solar_kwh: float,
    tariff_rate: float,
    subsidy_flag: bool,
) -> dict:
    """
    Compute a household's daily bill.

    Net billable consumption = max(0, consumption - solar_generation)
    (net metering: solar offsets consumption 1:1, cannot go negative/credit
    in this simplified model). Subsidized tiers get an extra 10% discount on
    the net amount, mirroring the `subsidy_flag` field's role from the brief.
    """
    if total_consumption_kwh < 0 or total_solar_kwh < 0 or tariff_rate < 0:
        raise ValueError("consumption, solar generation, and tariff_rate must be >= 0")

    net_consumption_kwh = max(0.0, total_consumption_kwh - total_solar_kwh)
    gross_amount = net_consumption_kwh * tariff_rate
    subsidy_discount = 0.10 if subsidy_flag else 0.0
    final_amount = gross_amount * (1 - subsidy_discount)
    solar_offset_kwh = min(total_consumption_kwh, total_solar_kwh)
    solar_self_sufficiency = (
        round(solar_offset_kwh / total_consumption_kwh, 4) if total_consumption_kwh > 0 else 0.0
    )

    return {
        "net_consumption_kwh": round(net_consumption_kwh, 4),
        "gross_amount": round(gross_amount, 2),
        "subsidy_discount_pct": subsidy_discount * 100,
        "final_amount": round(final_amount, 2),
        "solar_self_sufficiency_pct": round(solar_self_sufficiency * 100, 2),
    }


def renewable_share(power_consumption_kwh: float, solar_generation_kwh: float) -> float:
    """
    Fraction of a zone's/household's load that is currently served by solar,
    in [0, 1]. Used by both the streaming low-renewable alert rule and the
    batch zone_solar_daily report.
    """
    if power_consumption_kwh <= 0:
        return 0.0 if solar_generation_kwh <= 0 else 1.0
    return round(min(1.0, solar_generation_kwh / power_consumption_kwh), 4)
