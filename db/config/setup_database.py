"""
setup_database.py  (FINAL)
Put this file in:  waste-food-project/db/config/setup_database.py
Just run it (no arguments needed):
    python db/config/setup_database.py

Steps:
  0) create the database (from DATABASE_URL) if missing
  1) create tables           (db/queries/food_waste_queries.sql)
  2) load the 2 CSV files into the staging tables
  3) move data into the real tables
  4) print row counts + foreign-key checks

Requires:  pip install sqlalchemy psycopg2-binary python-dotenv
and a .env file (project root) containing DATABASE_URL
"""
import argparse
import csv
import io
import os
import sys
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

# ---------- paths (relative to the project root) ----------
ROOT = Path(__file__).resolve().parents[2]  # waste-food-project/

DEFAULT_SQL = ROOT / "db" / "queries" / "food_waste_queries.sql"
DEFAULT_FOOD_CSV = ROOT / "data" / "World-food-waste_cleaned_final.csv"
DEFAULT_ACTIVITY_CSV = ROOT / "data"/ "World-food-waste_activity_long.csv"

# load .env from the project root, or wherever it is found
if (ROOT / ".env").exists():
    load_dotenv(ROOT / ".env")
else:
    load_dotenv(find_dotenv())

STAGING_TABLES = {
    "staging.stg_food_waste": [
        "m49_code", "country", "cpc_code", "commodity", "year",
        "loss_percentage", "activity", "method_data_collection",
        "reference", "url", "record_id", "activity_count",
    ],
    "staging.stg_activity_long": ["record_id", "activity"],
}


def normalize(name: str) -> str:
    n = name.strip().lstrip("\ufeff").lower()
    for ch in (" ", "-", "/", "(", ")"):
        n = n.replace(ch, "_")
    while "__" in n:
        n = n.replace("__", "_")
    return n.strip("_")


def create_database_if_missing(url: str):
    target = make_url(url)
    db_name = target.database
    if not db_name:
        sys.exit("DATABASE_URL must include a database name")

    admin_engine = create_engine(
        target.set(database="postgres"), isolation_level="AUTOCOMMIT"
    )
    with admin_engine.connect() as c:
        exists = c.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": db_name}
        ).scalar()
        if exists:
            print(f"   database '{db_name}' already exists")
        else:
            c.execute(text(f'CREATE DATABASE "{db_name}"'))
            print(f"   database '{db_name}' created")
    admin_engine.dispose()


def split_sql(path: Path):
    sql = path.read_text(encoding="utf-8-sig")
    marker_load = "-- 2. Load country, commodity, method and activity"
    marker_checks7 = "-- 7. Check foreign key relationships"

    i_load = sql.index(marker_load)
    i_commit = sql.index("COMMIT;")
    i_check7 = sql.index(marker_checks7)

    ddl = sql[:i_load].replace("BEGIN;", "")
    load = sql[i_load:i_commit]
    counts = sql[i_commit + len("COMMIT;"):i_check7]
    fk_checks = sql[i_check7:]
    return ddl, load, counts, fk_checks


def copy_csv(cur, table: str, csv_path: Path):
    allowed = STAGING_TABLES[table]

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        cols = [normalize(h) for h in header]

        # map CSV columns -> staging columns (extra columns are ignored)
        keep = [(i, c) for i, c in enumerate(cols) if c in allowed]
        if "method_data_collection" in allowed and "method_data_collection" not in cols:
            for i, c in enumerate(cols):
                if "method" in c:
                    keep.append((i, "method_data_collection"))
                    break

        found = {c for _, c in keep}
        missing = [c for c in allowed if c not in found]
        ignored = [cols[i] for i in range(len(cols)) if i not in {k for k, _ in keep}]

        required = {"record_id", "activity"} if table.endswith("activity_long") else {
            "m49_code", "country", "cpc_code", "commodity", "year",
            "loss_percentage", "record_id",
        }
        if required - found:
            sys.exit(
                f"\nERROR: {csv_path.name} is missing required columns: "
                f"{sorted(required - found)}\nCSV columns: {cols}"
            )
        if missing:
            print(f"   note: columns not in CSV (left empty): {missing}")
        if ignored:
            print(f"   note: extra CSV columns ignored: {ignored}")

        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in reader:
            if not row:
                continue
            writer.writerow([row[i] if i < len(row) else "" for i, _ in keep])
        buf.seek(0)

    col_names = [c for _, c in keep]
    cur.execute(f"TRUNCATE TABLE {table};")  # safe to re-run
    cur.copy_expert(
        f"COPY {table} ({', '.join(col_names)}) FROM STDIN WITH (FORMAT csv)",
        buf,
    )
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    print(f"   loaded {cur.fetchone()[0]} rows into {table}")


def print_query(cur, sql: str):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(cols))
    for row in cur.fetchall():
        print("   " + " | ".join(str(v) for v in row))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sql", type=Path, default=DEFAULT_SQL)
    p.add_argument("--food-waste-csv", type=Path, default=DEFAULT_FOOD_CSV)
    p.add_argument("--activity-csv", type=Path, default=DEFAULT_ACTIVITY_CSV)
    args = p.parse_args()

    for label, path in (("SQL file", args.sql),
                        ("food waste CSV", args.food_waste_csv),
                        ("activity CSV", args.activity_csv)):
        if not path.exists():
            sys.exit(f"ERROR: {label} not found: {path}")

    url = os.getenv("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (check your .env file)")

    ddl, load, counts, fk_checks = split_sql(args.sql)

    print("[0/4] Creating the database if needed...")
    create_database_if_missing(url)

    engine = create_engine(url)
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()

        print("[1/4] Creating tables...")
        cur.execute(ddl)
        conn.commit()

        print("[2/4] Loading CSV files into staging tables...")
        copy_csv(cur, "staging.stg_food_waste", args.food_waste_csv)
        copy_csv(cur, "staging.stg_activity_long", args.activity_csv)
        conn.commit()

        print("[3/4] Moving data into the real tables...")
        cur.execute(load)
        conn.commit()

        print("[4/4] Checks")
        print("   Row counts:")
        print_query(cur, counts)
        print("   Foreign key checks (all should be 0):")
        print_query(cur, fk_checks)

        print("\nDone.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()