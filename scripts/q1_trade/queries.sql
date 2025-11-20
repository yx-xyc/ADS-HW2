-- Turn on timing (Required for full credit)
.timer on

-- (a) Weighted average price
SELECT '--- Query A ---' as label;
SELECT stocksymbol, SUM(price * quantity) / SUM(quantity) as weighted_avg
FROM trade GROUP BY stocksymbol ORDER BY stocksymbol LIMIT 10;

-- (b) 10-trade unweighted moving average
SELECT '--- Query B ---' as label;
SELECT stocksymbol, time, price,
    AVG(price) OVER (PARTITION BY stocksymbol ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as mov_avg
FROM trade ORDER BY stocksymbol, time LIMIT 20;

-- (c) 10-trade weighted moving average
SELECT '--- Query C ---' as label;
SELECT stocksymbol, time,
    SUM(price * quantity) OVER w / SUM(quantity) OVER w as weighted_mov_avg
FROM trade
WINDOW w AS (PARTITION BY stocksymbol ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
ORDER BY stocksymbol, time LIMIT 20;

-- (d) Best buy/sell (Max Profit)
SELECT '--- Query D ---' as label;
WITH min_prices AS (
    SELECT stocksymbol, price,
        MIN(price) OVER (PARTITION BY stocksymbol ORDER BY time) as min_prev
    FROM trade
)
SELECT stocksymbol, MAX(price - min_prev) as max_profit
FROM min_prices GROUP BY stocksymbol ORDER BY max_profit DESC LIMIT 20;