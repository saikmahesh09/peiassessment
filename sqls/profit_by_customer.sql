-- Profit by Customer
SELECT
    customer_name,
    SUM(total_profit) AS profit
FROM refine.default.profit_aggregated
GROUP BY customer_name
ORDER BY profit DESC;


