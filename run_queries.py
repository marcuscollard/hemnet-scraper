from __future__ import annotations

import os
from pathlib import Path

import psycopg2


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if not key or key in os.environ:
            continue
        os.environ[key.strip()] = value.strip().strip('"').strip("'")


def _load_statements(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    _load_env_file(Path(os.getenv("ENV_FILE", "")) if os.getenv("ENV_FILE") else base_dir / ".env")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("Missing DATABASE_URL. Set it in the environment or .env.")

    sql_path = base_dir / "queries.sql"
    if not sql_path.exists():
        raise SystemExit(f"Missing queries file: {sql_path}")

    statements = _load_statements(sql_path)
    if not statements:
        raise SystemExit("No SQL statements found in queries.sql.")

    connection = psycopg2.connect(database_url)
    connection.autocommit = True
    with connection:
        with connection.cursor() as cursor:
            for index, statement in enumerate(statements, start=1):
                print(f"\n-- Query {index} ------------------------------")
                print(statement)
                cursor.execute(statement)
                if cursor.description:
                    rows = cursor.fetchall()
                    print(f"Rows: {len(rows)}")
                    columns = [col.name for col in cursor.description]
                    for row in rows:
                        record = dict(zip(columns, row))
                        print(record)
                else:
                    print("OK")


if __name__ == "__main__":
    main()
