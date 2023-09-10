CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dwh_dataset }}.{{ params.table_name }}` AS
SELECT
  t1.id,
  t1.name,
  t1.category_id,
  t1.price,
  t1.inventory_status,
  CASE WHEN t1.stock_item_qty > 0 THEN TRUE ELSE FALSE END AS is_instock,
  SUM(t1.sale_amount) OVER(PARTITION BY t1.category_id) AS total_sales_amount
FROM (
  SELECT
    t2.id,
    t2.name,
    t2.categories.id AS category_id,
    t2.price,
    t2.inventory_status,
    t2.stock_item.qty AS stock_item_qty,
    SUM(t2.price * t2.quantity_sold.value) AS sale_amount
  FROM
     `{{ params.project_id }}.{{ params.staging_dataset }}.{{ params.table_name }}` AS t2
  WHERE
    t2.stock_item.qty > 0
  GROUP BY
    t2.id, t2.name, t2.categories.id, t2.price, t2.inventory_status, t2.stock_item.qty
) AS t1;