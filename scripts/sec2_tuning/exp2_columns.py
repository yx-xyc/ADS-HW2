import time
import psycopg2
import duckdb
import os
import random

# --- CONFIGURATION ---
PG_DSN = "dbname=hw2 user=yx2021 host=/home/yx2021/pgsql/data/run port=10000"
DUCK_DB_FILE = os.path.expanduser("~/duckdb_databases/hw2_sec2_tuning.duckdb")
ROW_COUNT = 100000

def get_pg_conn():
    try:
        return psycopg2.connect(PG_DSN)
    except Exception as e:
        print(f"[Error] Postgres connection failed: {e}")
        return None

def setup_table(system, num_cols):
    """Creates a table with num_cols columns and populates it"""
    cols = [f"col_{i} INTEGER" for i in range(num_cols)]
    col_def = ", ".join(cols)
    
    # Generate random values for one row
    row_vals = ",".join([str(random.randint(0,100)) for _ in range(num_cols)])
    
    if system == "duckdb":
        con = duckdb.connect(DUCK_DB_FILE)
        con.execute("DROP TABLE IF EXISTS col_test")
        con.execute(f"CREATE TABLE col_test ({col_def})")
        # Use DuckDB's range() to generate data fast
        con.execute(f"INSERT INTO col_test SELECT {row_vals} FROM range({ROW_COUNT})")
        con.close()
        
    elif system == "postgres":
        conn = get_pg_conn()
        if not conn: return
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS col_test")
        cur.execute(f"CREATE TABLE col_test ({col_def})")
        # Use generate_series() to generate data fast
        cur.execute(f"INSERT INTO col_test SELECT {row_vals} FROM generate_series(1, {ROW_COUNT})")
        conn.commit()
        cur.close()
        conn.close()

def run_query_test(system, query_type):
    start_time = time.time()
    
    if system == "duckdb":
        con = duckdb.connect(DUCK_DB_FILE)
        if query_type == "all":
            con.execute("SELECT * FROM col_test").fetchall()
        else:
            con.execute("SELECT col_0 FROM col_test").fetchall()
        con.close()
        
    elif system == "postgres":
        conn = get_pg_conn()
        if not conn: return 0
        cur = conn.cursor()
        if query_type == "all":
            cur.execute("SELECT * FROM col_test")
        else:
            cur.execute("SELECT col_0 FROM col_test")
        cur.fetchall() # Force data retrieval
        cur.close()
        conn.close()
        
    return time.time() - start_time

def run_experiment():
    print(f"{'System':<10} | {'Scenario':<10} | {'Query':<10} | {'Time (s)':<10}")
    print("-" * 55)
    
    # Strong: Wide Table (100 cols) - SELECT * should be slow
    print("--- Setting up Wide Table (Strong Case) ---")
    for sys in ["duckdb", "postgres"]:
        setup_table(sys, 100)
        t_all = run_query_test(sys, "all")
        t_one = run_query_test(sys, "needed")
        print(f"{sys:<10} | {'Strong':<10} | {'SELECT *':<10} | {t_all:.4f}")
        print(f"{sys:<10} | {'Strong':<10} | {'SELECT 1':<10} | {t_one:.4f}")

    print("-" * 55)

    # Weak: Narrow Table (2 cols) - SELECT * should be fast
    print("--- Setting up Narrow Table (Weak Case) ---")
    for sys in ["duckdb", "postgres"]:
        setup_table(sys, 2)
        t_all = run_query_test(sys, "all")
        t_one = run_query_test(sys, "needed")
        print(f"{sys:<10} | {'Weak':<10} | {'SELECT *':<10} | {t_all:.4f}")
        print(f"{sys:<10} | {'Weak':<10} | {'SELECT 1':<10} | {t_one:.4f}")

if __name__ == "__main__":
    if os.path.exists(DUCK_DB_FILE): os.remove(DUCK_DB_FILE)
    run_experiment()