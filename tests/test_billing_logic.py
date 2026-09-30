"""
Unit tests for the pure billing/renewable-share logic in
sources/simulation_logic.py. These are the "core pipeline logic" lines the
assignment brief says the student must be able to defend line-by-line, so
they're the most important thing to have solid tests for.
"""
import pytest

from sources.simulation_logic import compute_household_bill, renewable_share


class TestComputeHouseholdBill:
    def test_no_solar_full_price(self):
        bill = compute_household_bill(
            total_consumption_kwh=10.0, total_solar_kwh=0.0, tariff_rate=45.0, subsidy_flag=False
        )
        assert bill["net_consumption_kwh"] == 10.0
        assert bill["gross_amount"] == pytest.approx(450.0)
        assert bill["final_amount"] == pytest.approx(450.0)
        assert bill["subsidy_discount_pct"] == 0.0

    def test_solar_partially_offsets_consumption(self):
        bill = compute_household_bill(
            total_consumption_kwh=10.0, total_solar_kwh=4.0, tariff_rate=45.0, subsidy_flag=False
        )
        assert bill["net_consumption_kwh"] == 6.0
        assert bill["gross_amount"] == pytest.approx(270.0)
        assert bill["solar_self_sufficiency_pct"] == pytest.approx(40.0)

    def test_solar_exceeds_consumption_bills_zero_not_negative(self):
        # Net metering in this simplified model: no credit for excess export.
        bill = compute_household_bill(
            total_consumption_kwh=5.0, total_solar_kwh=8.0, tariff_rate=45.0, subsidy_flag=False
        )
        assert bill["net_consumption_kwh"] == 0.0
        assert bill["gross_amount"] == 0.0
        assert bill["final_amount"] == 0.0
        # self-sufficiency capped at 100% of what was actually consumed
        assert bill["solar_self_sufficiency_pct"] == pytest.approx(100.0)

    def test_subsidy_flag_applies_ten_percent_discount(self):
        bill = compute_household_bill(
            total_consumption_kwh=10.0, total_solar_kwh=0.0, tariff_rate=30.0, subsidy_flag=True
        )
        assert bill["gross_amount"] == pytest.approx(300.0)
        assert bill["final_amount"] == pytest.approx(270.0)  # 10% off
        assert bill["subsidy_discount_pct"] == 10.0

    def test_zero_consumption_no_division_by_zero(self):
        bill = compute_household_bill(
            total_consumption_kwh=0.0, total_solar_kwh=0.0, tariff_rate=45.0, subsidy_flag=False
        )
        assert bill["solar_self_sufficiency_pct"] == 0.0
        assert bill["final_amount"] == 0.0

    @pytest.mark.parametrize("bad_kwargs", [
        {"total_consumption_kwh": -1.0, "total_solar_kwh": 0.0, "tariff_rate": 45.0, "subsidy_flag": False},
        {"total_consumption_kwh": 1.0, "total_solar_kwh": -1.0, "tariff_rate": 45.0, "subsidy_flag": False},
        {"total_consumption_kwh": 1.0, "total_solar_kwh": 0.0, "tariff_rate": -5.0, "subsidy_flag": False},
    ])
    def test_negative_inputs_raise(self, bad_kwargs):
        with pytest.raises(ValueError):
            compute_household_bill(**bad_kwargs)


class TestRenewableShare:
    def test_normal_case(self):
        assert renewable_share(power_consumption_kwh=10.0, solar_generation_kwh=3.0) == 0.3

    def test_solar_exceeds_consumption_caps_at_one(self):
        assert renewable_share(power_consumption_kwh=2.0, solar_generation_kwh=5.0) == 1.0

    def test_zero_consumption_zero_solar(self):
        assert renewable_share(power_consumption_kwh=0.0, solar_generation_kwh=0.0) == 0.0

    def test_zero_consumption_nonzero_solar(self):
        # e.g. exporting at night edge case / meter noise -- treat as fully renewable
        assert renewable_share(power_consumption_kwh=0.0, solar_generation_kwh=1.0) == 1.0

    def test_below_low_renewable_threshold(self):
        from sources.common import LOW_RENEWABLE_THRESHOLD
        share = renewable_share(power_consumption_kwh=100.0, solar_generation_kwh=5.0)
        assert share < LOW_RENEWABLE_THRESHOLD
