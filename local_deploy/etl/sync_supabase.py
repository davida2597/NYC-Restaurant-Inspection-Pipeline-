import os
import time
import pandas as pd
from supabase import create_client
from sqlalchemy import create_engine, text

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAMES = ["inspections", "inspection_violations", "restaurants", "violations"]

def wait_for_supabase():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    for _ in range(10):
        try:
            supabase.table(TABLE_NAMES[0]).select("*").limit(1).execute()
            print("✅ Supabase reachable")
            return supabase
        except Exception as e:
            print(f"⏳ Waiting for Supabase... {e}")
            time.sleep(3)

    raise Exception("❌ Supabase not reachable")

def main():
    supabase = wait_for_supabase()

    engine = create_engine(
        f"postgresql+psycopg://{os.getenv('LOCAL_DB_USER')}:{os.getenv('LOCAL_DB_PASSWORD')}"
        f"@{os.getenv('LOCAL_DB_HOST')}:{os.getenv('LOCAL_DB_PORT')}/{os.getenv('LOCAL_DB_NAME')}"
    )

    # Drop etl_status once before the loop
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS etl_status"))

    # Fetch and load each table
    for table_name in TABLE_NAMES:
        response = supabase.table(table_name).select("*").execute()
        df = pd.DataFrame(response.data)

        # df.to_sql with if_exists="replace" handles drop+recreate automatically
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Create readiness flag inside Postgres
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE etl_status (
                ready BOOLEAN
            )
        """))
        conn.execute(text("INSERT INTO etl_status (ready) VALUES (TRUE)"))

    # File flag for Docker healthcheck
    with open("/tmp/etl_done", "w") as f:
        f.write("done")

    print("✅ ETL complete")

if __name__ == "__main__":
    main()