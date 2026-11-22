# CO2 Emissions Project

[![CI](https://github.com/ValantisChatzimagkas/CO2-emissions-project/actions/workflows/ci.yml/badge.svg)](https://github.com/ValantisChatzimagkas/CO2-emissions-project/actions/workflows/ci.yml)

Generates synthetic greenhouse gas (GHG) emissions data for a set of organizations, loads it into
PostgreSQL, and prints a summary report.

Scope 1 covers direct emissions from owned sources (e.g. fuel burned on site), Scope 2 covers
indirect emissions from purchased energy, and Scope 3 covers all other value-chain emissions
(e.g. business travel, waste, purchased goods).

## Data model

![Database schema](db_schema.png)

- `organizations`: the entities being tracked.
- `emission_sources`: activities that emit, each assigned to scope 1, 2 or 3 (unique per scope and name).
- `emission_factors`: kgCO2e per unit of activity for a source, with the calculation method.
- `emissions_logs`: dated activity records per organization and source; `total_emissions` is
  `quantity * factor_value` in kgCO2e.

The schema is defined only in `init.sql`, which Postgres runs on first start.

## Getting started

Requires Docker with Compose.

```bash
docker compose up --build
```

The generator starts once the database healthcheck passes, loads the data, and prints the report.
Postgres is exposed on host port `5433` by default; set `DB_PORT`, `DB_USER`, `DB_PASSWORD` or
`DB_NAME` in the environment or a `.env` file (see `.env.example`) to override. Remove everything with
`docker compose down -v`.

## Sample report

```text
Total emissions by scope (kgCO2e)
  Scope 1: 1,135,908.81  (402 logs)
  Scope 2:   331,525.14  (277 logs)
  Scope 3: 1,038,522.98  (321 logs)

Top 5 organizations by total emissions (kgCO2e)
  1. Callahan and Sons                  172,516.63
  2. Henderson, Ramirez and Lewis       156,906.38
  3. Arnold Ltd                         154,318.67
  4. Cox-Osborn                         147,479.62
  5. Wagner Inc                         140,641.18
```

The generator uses a fixed seed, so a run against an empty database with the locked dependencies reproduces this output.

## Running tests

```bash
uv run pytest -q
```

The tests cover the pure data-generation functions and need no database.

## Design decisions

- **Single schema source:** `init.sql` owns the DDL; the Python reflects the tables instead of redefining them.
- **One config path:** the database URL is built from `DB_*` environment variables; no secrets are committed.
- **Startup ordering:** a compose healthcheck with `depends_on: service_healthy` replaces in-code retries.
- **Pure generators:** `build_*` functions return plain dicts, and a single function does the I/O in one
  transaction, so a failed run leaves no partial data.
- **Consistent data:** each log's emissions derive from its source's emission factor, and sources are
  assigned to a fixed scope.
- **Reporting in SQL:** aggregates use `GROUP BY` in the database, not pandas.
- **Tooling:** dependencies are managed with uv (`pyproject.toml`, `uv.lock`); CI runs ruff and pytest.

## Limitations

- Data is synthetic; factors are random values between 0.1 and 10 kgCO2e per unit, not real-world factors.
- Not idempotent: each run appends new organizations, factors and logs (sources are de-duplicated).
- Emission factors are not versioned or tied to dates.
- No integration tests against a live database.

## License

[MIT](LICENSE)
