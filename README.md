# ADS-HW2

**Vincent(Yanchong) Xu - yx2021 - N13337595**

## Question 1 Trade

### 1.1 Data Generation

A python script is implemented to generate 10 million synthetic stock trades with fractal distribution following the specification. The source code could be found under:

 `ADS-HW2/scripts/q1_trade/gen_trade_data.py` 

In `gen_trade_data.py` , the method `gen_fractal_symbols(0.3, 70000)` is invoked to create a symbol pool of around 100,000 stock symbols. It is implemented following the pseudocode from instructions: repeated taking the top% of symbols and prepending them create power-law frequency distribution. By doing so, a realistic market where a few symbols trade much more frequently than others is produced. 

The trade data is generated using `generate_trades()` method with the schema: 

`(stock_symbol, time, quantity, price)`

The `time` is the primary key. It is unique integer from 1 to 10,000,000. The `quantity` is generated from uniform random distribution in range `[100. 10,000]`. The `price` is generated randomly between `[50, 500]`, then, for each trade, the price will fluctuate with deltas in range `[1, 5]` , ensuring successive prices for the same symbol differ by 5 at most and stay within the required ranges. The `stock_symbol` is selected uniformly from the fractal pool, creating interleaved trades. The output trade data would be stored as:

 `ADS-HW2/data/trade.csv` 

### 1.2 Query Implementation

I used `DuckDB` to execute the four analytical queries on the 10M row dataset. The queries are here:

`ADS-HW2/scripts/q1_trade/queries.sql` 

**(a) Weighted Average Price per stock**

```sql
SELECT stocksymbol, SUM(price * quantity) / SUM(quantity) as weighted_avg
FROM trade GROUP BY stocksymbol ORDER BY stocksymbol LIMIT 10;
```

The query computes the volume-weighted average price for each stock. 

**(b) 10-Trade Unweighted Moving Average**

```sql
SELECT stocksymbol, time, price,
    AVG(price) OVER (PARTITION BY stocksymbol 
		    ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as mov_avg
FROM trade ORDER BY stocksymbol, time LIMIT 20;
```

Uses window functions to calculate the rolling 10-trade average price (or fewer for the first few trades).

**(c) 10-Trade Weighted Moving Average**

```sql
SELECT stocksymbol, time,
    SUM(price * quantity) OVER w / SUM(quantity) OVER w as weighted_mov_avg
FROM trade
WINDOW w AS (PARTITION BY stocksymbol 
				ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
ORDER BY stocksymbol, time LIMIT 20;
```

Similar to (b), but weights each price by its trade quantity using a named window definition. 

**(d) Best Buy-First/Sell-Later profit**

```sql
WITH min_prices AS (
    SELECT stocksymbol, price,
        MIN(price) OVER (PARTITION BY stocksymbol ORDER BY time) as min_prev
    FROM trade
)
SELECT stocksymbol, MAX(price - min_prev) as max_profit
FROM min_prices GROUP BY stocksymbol ORDER BY max_profit DESC LIMIT 20;
```

For each stock, computes the running minimum price up to the trade, then finds the maximum difference (sell price - minimum prior buy price). 

### 1.3 Results and Performance

The execution log could be found under:

`ADS-HW2/scripts/q1_trade/q1_typescript.txt`

All queries completed well under the 2-minute-requirement on the NYU compute server (crunchy2):

| Query | Description | Execution Time |
| --- | --- | --- |
| A | Weighted Average | 0.922s |
| B | Unweighted Moving Avg | 1.910s |
| C | Weighted Moving Avg | 2.944s |
| D | Best Buy/Sell Profit | 2.064s |

From the above data, we observed that DuckDB’s columnar storage and vectorized execution handle 10M rows efficient. Window functions scaled well with data partitioning by stock symbol. Query C was slowest due to computing two window aggregates per row. 

## Question 2

### 2.1 Experiment Selection

The two tuning experiments I selected to test DuckDB and PostgreSQL are:

- Batch Loading VS Row-by-Row Insertion - testing bulk insert performance
- SELECT * VS SELECT specific Columns - testing column projection efficiency

These 2 experiments were chosen because they represent common database operations (loading and querying). Both of them have clear strong/weak scenarios based on data volume and table width. 

The source code of these two experiment are:

- `ADS-HW2/scripts/q2_tuning/exp1_loading.py`
- `ADS-HW2/scripts/q2_tuning/exp2_columns.py`

### 2.2 Experiment 1 - Batch Loading VS Row-by-Row Insertion

**Hypothesis:** 

Batch loading should significantly outperform row-by-row insertion due to reduced transaction overhead and optimized buffer management. 

**Method:** 

We are going to test both Strong and Weak cases (50,000 rows VS 50 rows) using both row-by-row and batch insertion on both PostgreSQL and DuckDB. 

**Results:**

| System | Scenario | Method | Time (s) | Improvement |
| --- | --- | --- | --- | --- |
| PostgreSQL | Strong (50K) | Row-by-row | 49.80 | - |
| PostgreSQL | Strong (50K) | Batch | 6.01 | 8.3x faster |
| PostgreSQL | Weak (50) | Row-by-row | 0.10 | - |
| PostgreSQL | Weak (50) | Batch | 0.05 | 2.0x faster |
| DuckDB | Strong (50K) | Row-by-row | 98.51 | - |
| DuckDB | Strong (50K) | Batch | 75.34 | 1.3x faster |
| DuckDB | Weak (50) | Row-by-row | 0.20 | - |
| DuckDB | Weak (50) | Batch | 0.17 | 1.2x faster |

**Observation:**

PostgreSQL shows an 8.3x improvement in the strong case. Each row-by-row commit triggers WAL writes, fsync, and transaction overhead. Batching amortizes these costs across 50K rows. DuckDB shows only 1.3x improvement because its in-process architecture lacks the client-server and transaction overhead that batching is designed to avoid.

In the weak case, both systems show minimal improvement (1.2-2.0x) because the fixed overhead of batch preparation approaches the total execution time for 50 rows.

**Conclusion:**

Batch loading provides up to 8.3x speedup on client-server databases for datasets >10K rows due to reduced transaction commit overhead. In-process databases show smaller gains (1.2-1.5x). For datasets <100 rows, batching provides minimal benefit.

### 2.2 Experiment 2 - SELECT * VS SELECT specific Columns

**Hypothesis:**

Selecting only needed columns should be faster than SELECT *, especially for wide tables. 

**Method:**

We are going to test both Strong and Weak cases (100 columns VS 2 columns) using both SELECT * and SELECT col_0 on both PostgreSQL and DuckDB. 

**Results:**

| System | Scenario | Query | Time (s) | Improvement |
| --- | --- | --- | --- | --- |
| PostgreSQL | Strong (100 cols) | SELECT * | 2.87 | - |
| PostgreSQL | Strong (100 cols) | SELECT col_0 | 0.10 | 28.7x faster |
| PostgreSQL | Weak (2 cols) | SELECT * | 0.14 | - |
| PostgreSQL | Weak (2 cols) | SELECT col_0 | 0.08 | 1.8x faster |
| DuckDB | Strong (100 cols) | SELECT * | 2.08 | - |
| DuckDB | Strong (100 cols) | SELECT col_0 | 0.17 | 12.5x faster |
| DuckDB | Weak (2 cols) | SELECT * | 0.15 | - |
| DuckDB | Weak (2 cols) | SELECT col_0 | 0.13 | 1.2x faster |

**Observation:**

PostgreSQL achieves 28.7x improvement in the strong case. Row-oriented storage must read entire rows, project unwanted columns, serialize them, and transfer over the network. DuckDB shows 12.5x improvement: its columnar storage only reads col_0 from disk, avoiding I/O for 99 columns. The smaller gain reflects DuckDB's in-process architecture eliminating network overhead.

In the weak case, both systems show minimal difference (1.2-1.8x) because the overhead of fetching one extra column is negligible.

**Conclusion:**

Column selection provides up to 28.7x speedup for wide tables (>50 columns). Row-oriented systems show larger gains due to network transfer overhead; columnar databases benefit from reduced I/O. For narrow tables (<5 columns), SELECT * has minimal penalty (<2x).

## Question 3

### 3.1 Problem Statement

Given three tables representing a social network—like(person, artist), dislike(person, artist), and friend(person1, person2)—the goal is to generate friend-based artist recommendations. The challenge requires producing three outputs:

1. MyFriendLikes(u1, u2, a): User u1 has friend u2 who likes artist a, but u1 doesn't yet like or dislike a
2. MyFriendDislikes(u1, u2, a): User u1 has friend u2 who dislikes artist a, but u1 doesn't yet like or dislike a
3. IShouldLike(u1, a): User u1 should like artist a if at least one friend likes a and no friend of u1 dislikes a

All queries must complete in under 2 minutes on the CIMS compute servers, including index creation time. 

The related data, code, and log file could be found at:

- Source code: `ADS-HW2/q3_social/social_network_postgres.py`
- Execution log: `ADS-HW2/q3_social/q3_typescript.txt`
- Input Data: `ADS-HW2/data/*.txt`

### 3.2 Method

I used PostgreSQL with the following optimization strategy:

**Data Loading:**

- Used `COPY` command for bulk import instead of using `INSERT`
- Loaded raw data before creating indexes to avoid index maintenance overhead during insertion

**Friend Table Symmetrization:**

The friend relationship is bidirectional, but raw data only stores one direction.  I created a symmetric table with below query:

```sql
CREATE TABLE friends AS
SELECT p1, p2 FROM friends_raw
UNION ALL
SELECT p2 as p1, p1 as p2 FROM friends_raw
```

By doing so, the table size is doubled, but the expensive `UNION` operation is avoided in queries. 

**Indexing:**

Three indexes are created after loading data:

- `idx_friends_p1(p1, p2)` - Fast friend lookups
- `idx_likes_person(person, artist)` - speedup like checks
- `idx_dislikes_person(person, artist)` - Accelerate dislike checks

The (person, artist) column ordering enables fast filtering: queries search by person first, then check specific artists.

### 3.3 Query Implementation

**(a) MyFriendLikes**

```sql
CREATE TABLE MyFriendLikes AS
SELECT DISTINCT f.p1 as u1, f.p2 as u2, l.artist
FROM friends f
JOIN likes l ON f.p2 = l.person
WHERE NOT EXISTS (
  SELECT 1 FROM likes l2 WHERE l2.person = f.p1 AND l2.artist = l.artist
)
AND NOT EXISTS (
  SELECT 1 FROM dislikes d WHERE d.person = f.p1 AND d.artist = l.artist
);
```

This query finds all artists that a user's friends like but the user has no opinion on. The two `NOT EXISTS` clauses exclude artists the user already likes or dislikes.

**(b) MyFriendDislikes**

```sql
CREATE TABLE MyFriendDislikes AS
SELECT DISTINCT f.p1 as u1, f.p2 as u2, d.artist
FROM friends f
JOIN dislikes d ON f.p2 = d.person
WHERE NOT EXISTS (
    SELECT 1 FROM likes l2 WHERE l2.person = f.p1 AND l2.artist = d.artist
)
AND NOT EXISTS (
    SELECT 1 FROM dislikes d2 WHERE d2.person = f.p1 AND d2.artist = d.artist
);
```

This query mirrors MyFriendLikes logic for dislikes, identifying artists that friends dislike but the user has no opinions. Same `NOT EXISTS` optimization for efficient exclusion checks. 

**(c) IShouldLike**

```sql
CREATE TABLE IShouldLike AS
SELECT mfl.u1, mfl.artist
FROM MyFriendLikes mfl
LEFT JOIN MyFriendDislikes mfd
    ON mfl.u1 = mfd.u1 AND mfl.artist = mfd.artist
WHERE mfd.u1 IS NULL
GROUP BY mfl.u1, mfl.artist;
```

This queries recommends artists where at least one friend likes it and no friends dislike it. The `LEFT JOIN` with `WHERE NULL` efficiently identifies artists present only in MyFriendLikes using index lookups. 

### 3.4 Results

**performance Breakdown:**

- Setup & Indexing: 10.67s
- Query Execution: ~101s

**Output Results:**

| Table | Row Count | Sample Output |
| --- | --- | --- |
| MyFriendLikes | 7,174,700 | (1, 22953, 1), (1, 3385, 2), (1, 14598, 3) |
| MyFriendDislikes | 6,963,890 | (1, 28874, 1), (1, 27184, 1), (1, 24774, 5) |
| IShouldLike | 2,196,781 | (1, 2), (1, 3), (1, 12), (1, 13), (1, 20) |