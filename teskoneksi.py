from dotenv import load_dotenv
load_dotenv()
import os, psycopg2

conn = psycopg2.connect(os.environ["AUTOMETRIC_DB_URL"])
cur = conn.cursor()
cur.execute("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema='l1_silver'")
print("hypertables:", cur.fetchone()[0])   # harus 5
cur.execute("SELECT count(*) FROM l1_silver.unified_post")
print("post rows:", cur.fetchone()[0])
conn.close()