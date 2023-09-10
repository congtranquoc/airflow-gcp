CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dwh_dataset }}.newegg_data` AS
SELECT
    item_id,
    brand,
    rating,
    rating_num,
    price,
    price_shipping,
    image_url
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.newegg_data`




