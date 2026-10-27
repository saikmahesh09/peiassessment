-- Profit by Customer and Year
SELECT
    customer_name,
    order_year,
    SUM(total_profit) AS profit
FROM refine.default.profit_aggregated
GROUP BY customer_name, order_year
ORDER BY customer_name, order_year;
