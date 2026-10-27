-- Profit by Year
SELECT
    order_year,
    SUM(total_profit) AS profit
FROM refine.default.profit_aggregated
GROUP BY order_year
ORDER BY order_year;
