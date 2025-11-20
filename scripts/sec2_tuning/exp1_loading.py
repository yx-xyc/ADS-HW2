import time
import psycopg2
import duckdb
import os

# --- CONFIGURATION ---
# Your specific Postgres connection from the screenshot
PG_DSN = "dbname=hw2 user=yx2021 host=/home/yx2021/pgsql/data/run port=10000"

# Use a separate file for tuning to avoid locking your main hw2.duckdb
DUCK_DB_FILE = os.path.expanduser("~/duckdb_databases/hw2_sec2_tuning.duckdb")

# Scenarios
ROWS_STRONG = 50000   # Large load: Batching should win significantly
ROWS_WEAK = 50        # Tiny load: Batching overhead might make it slower/equal

def get_pg_conn():
    try:
        return psycopg2.connect(PG_DSN)
    except Exception as e:
        print(f"[Error] Postgres connection failed: {e}")
        return None

def run_loading_test(system, method, row_count):
    # Generate dummy data in memory
    data = [(i, f"val_{i}") for i in range(row_count)]
    
    start_time = time.time()
    
    if system == "duckdb":
        con = duckdb.connect(DUCK_DB_FILE)
        con.execute("DROP TABLE IF EXISTS load_test")
        con.execute("CREATE TABLE load_test (id INTEGER, v VARCHAR)")
        
        if method == "row":
            for row in data:
                # Insert one by one
                con.execute(f"INSERT INTO load_test VALUES ({row[0]}, '{row[1]}')")
        elif method == "batch":
            # Insert all at once
            con.executemany("INSERT INTO load_test VALUES (?, ?)", data)
        
        con.close()

    elif system == "postgres":
        conn = get_pg_conn()
        if not conn: return 0
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS load_test")
        cur.execute("CREATE TABLE load_test (id INTEGER, v VARCHAR)")
        
        if method == "row":
            for row in data:
                cur.execute("INSERT INTO load_test VALUES (%s, %s)", row)
            conn.commit() # Commit every row (simulating bad application logic)
        elif method == "batch":
            cur.executemany("INSERT INTO load_test VALUES (%s, %s)", data)
            conn.commit() # Commit once at the end
            
        cur.close()
        conn.close()

    return time.time() - start_time

def run_experiment():
    print(f"{'System':<10} | {'Scenario':<10} | {'Method':<10} | {'Time (s)':<10}")
    print("-" * 55)
    
    for rows, name in [(ROWS_STRONG, "Strong"), (ROWS_WEAK, "Weak")]:
        for sys in ["duckdb", "postgres"]:
            t_row = run_loading_test(sys, "row", rows)
            t_batch = run_loading_test(sys, "batch", rows)
            print(f"{sys:<10} | {name:<10} | {'Row':<10} | {t_row:.4f}")
            print(f"{sys:<10} | {name:<10} | {'Batch':<10} | {t_batch:.4f}")

if __name__ == "__main__":
    # Clean up before starting to ensure fresh state
    if os.path.exists(DUCK_DB_FILE):
        os.remove(DUCK_DB_FILE)
    run_experiment()