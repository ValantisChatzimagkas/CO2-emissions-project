"""Print a summary of the generated data using plain SQL aggregates."""

from sqlalchemy import text

from synthetic_data_generation.db import get_engine

TOP_N = 5

BY_SCOPE = text("""
    SELECT s.scope, COUNT(*) AS logs, SUM(l.total_emissions) AS total
    FROM emissions_logs l
    JOIN emission_sources s ON s.id = l.source_id
    GROUP BY s.scope
    ORDER BY s.scope
""")

TOP_ORGANIZATIONS = text("""
    SELECT o.name, SUM(l.total_emissions) AS total
    FROM emissions_logs l
    JOIN organizations o ON o.id = l.organization_id
    GROUP BY o.id, o.name
    ORDER BY total DESC
    LIMIT :top_n
""")


def main() -> None:
    with get_engine().connect() as conn:
        print("Total emissions by scope (kgCO2e)")
        for scope, logs, total in conn.execute(BY_SCOPE):
            print(f"  Scope {scope}: {total:>12,.2f}  ({logs} logs)")
        print(f"\nTop {TOP_N} organizations by total emissions (kgCO2e)")
        for rank, (name, total) in enumerate(conn.execute(TOP_ORGANIZATIONS, {"top_n": TOP_N}), 1):
            print(f"  {rank}. {name:<32} {total:>12,.2f}")


if __name__ == "__main__":
    main()
