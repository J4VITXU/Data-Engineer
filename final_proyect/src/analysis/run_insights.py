from pathlib import Path
import duckdb

DB_PATH = Path("warehouse/dw.duckdb")
SQL_PATH = Path("sql/insights.sql")

def main():
    con = duckdb.connect(str(DB_PATH))
    sql = SQL_PATH.read_text(encoding="utf-8")

    queries = [q.strip() for q in sql.split(";") if q.strip()]

    for i, q in enumerate(queries, 1):
        print(f"\n--- Insight {i} ---")
        df = con.execute(q).fetchdf()
        print(df.to_string(index=False))

    con.close()

if __name__ == "__main__":
    main()
