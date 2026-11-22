"""Generate synthetic GHG emissions data and load it into PostgreSQL.

The schema is owned by init.sql; tables are reflected here, never redefined.
The build_* functions are pure so they can be tested without a database.
"""

import random
from datetime import date, timedelta

from faker import Faker
from loguru import logger
from sqlalchemy import MetaData, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from synthetic_data_generation.db import get_engine

SEED = 42
NUM_ORGANIZATIONS = 20
NUM_LOGS = 1000
QUANTITY_RANGE = (1, 1000)
FACTOR_RANGE = (0.1, 10.0)
FACTOR_UNIT = "kgCO2e/unit"  # total_emissions is therefore in kgCO2e
LOG_DATE_RANGE = (date(2020, 1, 1), date(2025, 12, 31))
CALCULATION_METHODS = [
    "Method A: Direct Measurement",
    "Method B: Emission Factor Multiplication",
    "Method C: Hybrid Approach",
]

SOURCES_BY_SCOPE = {
    "1": ["Natural Gas", "Diesel Fuel", "Gasoline", "Coal", "Company Vehicles", "Refrigerants"],
    "2": ["Purchased Electricity", "Purchased Steam", "Purchased Heating", "Purchased Cooling"],
    "3": ["Air Travel", "Commuting", "Maritime Shipping", "Waste Disposal", "Purchased Goods"],
}


def build_organizations(fake: Faker, count: int) -> list[dict]:
    return [{"name": fake.company(), "description": fake.bs()} for _ in range(count)]


def build_emission_sources(fake: Faker) -> list[dict]:
    return [
        {"scope": scope, "source_name": name, "description": fake.bs()}
        for scope, names in SOURCES_BY_SCOPE.items()
        for name in names
    ]


def build_emission_factors(source_ids: list[int], rng: random.Random) -> list[dict]:
    return [
        {
            "source_id": source_id,
            "factor_value": round(rng.uniform(*FACTOR_RANGE), 3),
            "unit": FACTOR_UNIT,
            "calculation_method": rng.choice(CALCULATION_METHODS),
        }
        for source_id in source_ids
    ]


def build_emission_logs(
    count: int, org_ids: list[int], factors: dict[int, float], rng: random.Random
) -> list[dict]:
    """Create logs where total_emissions = quantity * the source's emission factor."""
    start, end = LOG_DATE_RANGE
    span_days = (end - start).days
    source_ids = list(factors)
    logs = []
    for _ in range(count):
        source_id = rng.choice(source_ids)
        quantity = rng.randint(*QUANTITY_RANGE)
        logs.append(
            {
                "source_id": source_id,
                "organization_id": rng.choice(org_ids),
                "date": start + timedelta(days=rng.randint(0, span_days)),
                "quantity": quantity,
                "total_emissions": round(quantity * factors[source_id], 2),
            }
        )
    return logs


def load(seed: int = SEED) -> None:
    """Generate and insert all data in a single transaction."""
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    engine = get_engine()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    orgs, sources, factors_t, logs_t = (
        metadata.tables[name]
        for name in ("organizations", "emission_sources", "emission_factors", "emissions_logs")
    )

    with engine.begin() as conn:
        org_ids = conn.execute(
            insert(orgs).returning(orgs.c.id), build_organizations(fake, NUM_ORGANIZATIONS)
        ).scalars().all()
        logger.info("Inserted {} organizations", len(org_ids))

        conn.execute(
            pg_insert(sources).on_conflict_do_nothing(), build_emission_sources(fake)
        )
        source_ids = conn.execute(select(sources.c.id)).scalars().all()
        logger.info("Emission sources available: {}", len(source_ids))

        factors = build_emission_factors(source_ids, rng)
        conn.execute(insert(factors_t), factors)
        factor_by_source = {f["source_id"]: f["factor_value"] for f in factors}
        logger.info("Inserted {} emission factors", len(factors))

        logs = build_emission_logs(NUM_LOGS, org_ids, factor_by_source, rng)
        conn.execute(insert(logs_t), logs)
        logger.info("Inserted {} emission logs", len(logs))


if __name__ == "__main__":
    load()
