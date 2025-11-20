import psycopg2
import time
import os

# --- CONFIGURATION ---
# Using your working connection string from Phase 2
PG_DSN = "dbname=hw2 user=yx2021 host=/home/yx2021/pgsql/data/run port=10000"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../../data")

def run_social_challenge():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        print("--- Connected to Postgres ---")
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    start_total = time.time()

    # 1. CLEANUP & SETUP
    # We drop ALL tables (inputs and outputs) to ensure a clean run every time.
    print("--- Cleaning up previous tables ---")
    cur.execute("""
        DROP TABLE IF EXISTS MyFriendLikes CASCADE;
        DROP TABLE IF EXISTS MyFriendDislikes CASCADE;
        DROP TABLE IF EXISTS IShouldLike CASCADE;
        DROP TABLE IF EXISTS likes CASCADE;
        DROP TABLE IF EXISTS dislikes CASCADE;
        DROP TABLE IF EXISTS friends_raw CASCADE;
        DROP TABLE IF EXISTS friends CASCADE;
    """)

    # Create Input Tables
    # We use UNLOGGED tables to speed up writing (durability doesn't matter for temp results)
    print("--- Loading Raw Data ---")
    cur.execute("""
        CREATE UNLOGGED TABLE likes (person INT, artist INT);
        CREATE UNLOGGED TABLE dislikes (person INT, artist INT);
        CREATE UNLOGGED TABLE friends_raw (p1 INT, p2 INT);
    """)

    # Load using \COPY equivalent (copy_expert)
    try:
        with open(os.path.join(DATA_DIR, 'like.txt'), 'r') as f:
            cur.copy_expert("COPY likes FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
        
        with open(os.path.join(DATA_DIR, 'dislike.txt'), 'r') as f:
            cur.copy_expert("COPY dislikes FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
            
        with open(os.path.join(DATA_DIR, 'friends.txt'), 'r') as f:
            cur.copy_expert("COPY friends_raw FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
    except FileNotFoundError as e:
        print(f"Error: Could not find data file. {e}")
        return

    # 2. Symmetrize Friends & Indexing
    # The prompt counts index creation time, so we time this carefully.
    print("--- Optimizing & Indexing (Crucial for Join Speed) ---")
    
    # Materialize symmetric friends: p1->p2 AND p2->p1
    cur.execute("""
        CREATE UNLOGGED TABLE friends AS 
        SELECT p1, p2 FROM friends_raw
        UNION ALL
        SELECT p2 as p1, p1 as p2 FROM friends_raw
    """)
    
    # CREATE INDEXES: This is the "Tuning" part that makes Postgres fast.
    # We need indexes on join columns to avoid sequential scans.
    cur.execute("CREATE INDEX idx_friends_p1 ON friends(p1, p2)")
    cur.execute("CREATE INDEX idx_likes_person ON likes(person, artist)")
    cur.execute("CREATE INDEX idx_dislikes_person ON dislikes(person, artist)")
    # Indexes for the inverse lookups in NOT EXISTS clauses
    cur.execute("CREATE INDEX idx_likes_artist ON likes(artist)") 
    
    print(f"Setup & Indexing Complete. Time so far: {time.time() - start_total:.2f}s")

    # 3. Execute Logic
    print("--- Executing Analysis Queries ---")
    
    # (i) MyFriendLikes: Friend u2 likes 'a', u1 does not
    cur.execute("""
        CREATE UNLOGGED TABLE MyFriendLikes AS
        SELECT DISTINCT f.p1 as u1, f.p2 as u2, l.artist
        FROM friends f
        JOIN likes l ON f.p2 = l.person
        WHERE NOT EXISTS (
            SELECT 1 FROM likes l2 WHERE l2.person = f.p1 AND l2.artist = l.artist
        )
        AND NOT EXISTS (
            SELECT 1 FROM dislikes d WHERE d.person = f.p1 AND d.artist = l.artist
        )
    """)

    # (ii) MyFriendDislikes: Friend u2 dislikes 'a', u1 does not
    cur.execute("""
        CREATE UNLOGGED TABLE MyFriendDislikes AS
        SELECT DISTINCT f.p1 as u1, f.p2 as u2, d.artist
        FROM friends f
        JOIN dislikes d ON f.p2 = d.person
        WHERE NOT EXISTS (
            SELECT 1 FROM likes l2 WHERE l2.person = f.p1 AND l2.artist = d.artist
        )
        AND NOT EXISTS (
            SELECT 1 FROM dislikes d2 WHERE d2.person = f.p1 AND d2.artist = d.artist
        )
    """)

    # (iii) IShouldLike: Recommendation (Friend likes, NO friend dislikes)
    cur.execute("""
        CREATE UNLOGGED TABLE IShouldLike AS
        SELECT mfl.u1, mfl.artist
        FROM MyFriendLikes mfl
        LEFT JOIN MyFriendDislikes mfd 
            ON mfl.u1 = mfd.u1 AND mfl.artist = mfd.artist
        WHERE mfd.u1 IS NULL
        GROUP BY mfl.u1, mfl.artist
    """)

    end_total = time.time()
    total_duration = end_total - start_total
    
    print(f"--- FINISHED ---")
    print(f"Total Execution Time (Load + Index + Query): {total_duration:.4f}s")
    
    if total_duration > 120:
        print("WARNING: Over 2 minute limit!")
    else:
        print("SUCCESS: Under 2 minute limit.")

    # 4. Verify Output
    print("\nSample Output (IShouldLike):")
    cur.execute("SELECT * FROM IShouldLike ORDER BY u1, artist LIMIT 5")
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_social_challenge()