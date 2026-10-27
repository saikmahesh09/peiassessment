-- Profit by Year and Product Category
SELECT
    order_year,
    category,
    SUM(total_profit) AS profit
FROM refine.default.profit_aggregated
GROUP BY order_year, category
ORDER BY order_year, category;
