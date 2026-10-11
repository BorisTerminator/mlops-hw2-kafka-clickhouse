-- П.3. Категория наибольшей транзакции по каждому штату.
-- argMax(cat_id, amount) возвращает cat_id из строки с максимальным amount в группе.
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM default.transactions
GROUP BY us_state
ORDER BY us_state;
