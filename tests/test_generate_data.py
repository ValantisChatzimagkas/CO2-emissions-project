import random

import pytest
from faker import Faker

from synthetic_data_generation import generate_data as gd


@pytest.fixture
def rng():
    return random.Random(1)


@pytest.fixture
def fake():
    f = Faker()
    f.seed_instance(1)
    return f


def test_organizations_shape(fake):
    orgs = gd.build_organizations(fake, 5)
    assert len(orgs) == 5
    assert all(set(o) == {"name", "description"} and o["name"] for o in orgs)


def test_emission_sources_cover_scopes_without_duplicates(fake):
    sources = gd.build_emission_sources(fake)
    assert {s["scope"] for s in sources} == {"1", "2", "3"}
    keys = [(s["scope"], s["source_name"]) for s in sources]
    assert len(keys) == len(set(keys))


def test_emission_factors_in_range(rng):
    factors = gd.build_emission_factors([1, 2, 3], rng)
    assert [f["source_id"] for f in factors] == [1, 2, 3]
    low, high = gd.FACTOR_RANGE
    assert all(low <= f["factor_value"] <= high for f in factors)
    assert all(f["calculation_method"] in gd.CALCULATION_METHODS for f in factors)


def test_emission_logs_values(rng):
    factors = {10: 2.0, 11: 0.5}
    logs = gd.build_emission_logs(200, [1, 2], factors, rng)
    start, end = gd.LOG_DATE_RANGE
    assert len(logs) == 200
    for log in logs:
        assert log["source_id"] in factors
        assert log["organization_id"] in (1, 2)
        assert start <= log["date"] <= end
        assert gd.QUANTITY_RANGE[0] <= log["quantity"] <= gd.QUANTITY_RANGE[1]
        assert log["total_emissions"] == pytest.approx(
            log["quantity"] * factors[log["source_id"]], abs=0.01
        )


def test_generation_is_reproducible():
    def run():
        r = random.Random(7)
        return gd.build_emission_logs(10, [1, 2, 3], {1: 1.0, 2: 2.0}, r)

    assert run() == run()
