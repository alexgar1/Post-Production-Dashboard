import argparse
from pathlib import Path

from db_helpers import get_connection


def apply_schema(schema_path: Path):
    sql = schema_path.read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)


def main():
    parser = argparse.ArgumentParser(description="Initialize the Monday Postgres schema.")
    parser.add_argument(
        "--schema",
        default="schema.sql",
        help="Path to the SQL schema file (default: schema.sql).",
    )
    args = parser.parse_args()
    schema_path = Path(args.schema).resolve()
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    apply_schema(schema_path)
    print(f"Schema applied from {schema_path}")


if __name__ == "__main__":
    main()
