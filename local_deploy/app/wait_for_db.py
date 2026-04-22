import os
import time
import psycopg

for i in range(30):
    try:
        conn = psycopg.connect(
            host=os.getenv("LOCAL_DB_HOST"),
            port=os.getenv("LOCAL_DB_PORT"),
            dbname=os.getenv("LOCAL_DB_NAME"),
            user=os.getenv("LOCAL_DB_USER"),
            password=os.getenv("LOCAL_DB_PASSWORD"),
        )
        conn.close()
        print("✅ Postgres reachable")
        break
    except Exception as e:
        print(f"⏳ Waiting for Postgres... {e}")
        time.sleep(2)
else:
    raise Exception("❌ Could not connect to Postgres")