SET enable_case_sensitive_identifier TO true;

-- create arificial table view allowed product categories (exclude Office Supplies)
-- this table is used to define values to excluded from further processing (to simulate invalid records)
DROP TABLE IF EXISTS dev.silver.product_allowed_categories;
CREATE TABLE dev.silver.product_allowed_categories AS 
SELECT 
    distinct CAST(category AS VARCHAR) AS category,
    current_timestamp AS updated_at
FROM dev.silver.temus_product_splitted 
WHERE category != 'Office Supplies'

-- split payload column into individual columns
DROP VIEW IF EXISTS dev.silver.temus_product_splitted;
CREATE VIEW dev.silver.temus_product_splitted AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    refresh_time,
    payload."Item" AS item,
    payload."Category" AS category,
    payload."Vendor" AS vendor,
    payload."Sale_Price" AS sale_price,
    payload."Stock_Status" AS stock_status
FROM dev.bronze.temus_product;

-- create cleaned table
-- update data types
-- remove duplicates - keep only records that occur once
-- remove invalid records: only allowed product categories (artifical filtering)
DROP VIEW IF EXISTS dev.silver.temus_product_cleaned;
CREATE VIEW dev.silver.temus_product_cleaned AS WITH deduplicate AS (
    select
        item,
        category,
        vendor,
        approximate_arrival_timestamp,
        AVG(sale_price) AS sale_price,
        ANY_VALUE(stock_status) AS stock_status
    FROM
        dev.silver.temus_product_splitted
    GROUP BY
        item,
        category,
        vendor,
        approximate_arrival_timestamp
    HAVING
        count(1) = 1
),
clean_str AS (
    SELECT
        REPLACE(CAST(item AS VARCHAR), '"', '') AS item,
        REGEXP_REPLACE(
            REPLACE(CAST(item AS VARCHAR), '"', ''),
            '[0-9]',
            ''
        ) AS item_category,
        REPLACE(CAST(category AS VARCHAR), '"', '') AS category,
        REPLACE(CAST(vendor AS VARCHAR), '"', '') AS vendor,
        REPLACE(CAST(stock_status AS VARCHAR), '"', '') AS stock_status,
        approximate_arrival_timestamp,
        ROUND(CAST(sale_price AS FLOAT4), 2) AS sale_price,
        DATE(approximate_arrival_timestamp) AS arrival_date
    FROM
        deduplicate
)
SELECT
    *
FROM
    clean_str
WHERE
    category IN (
        SELECT
            category
        FROM
            dev.silver.product_allowed_categories
    )
    AND sale_price > 0 

-- move invalid records to quarantine  
DROP VIEW IF EXISTS dev.silver.temus_product_quarantine;
CREATE VIEW dev.silver.temus_product_quarantine AS WITH deduplicate AS (
    SELECT
        item,
        category,
        vendor,
        approximate_arrival_timestamp,
        AVG(sale_price) AS sale_price,
        COUNT(1) AS n_records
    FROM
        dev.silver.temus_product_splitted
    GROUP BY
        item,
        category,
        vendor,
        approximate_arrival_timestamp
),
clean_str AS (
    SELECT
        REPLACE(CAST(item AS VARCHAR), '"', '') AS item,
        REPLACE(CAST(category AS VARCHAR), '"', '') AS category,
        REPLACE(CAST(vendor AS VARCHAR), '"', '') AS vendor,
        approximate_arrival_timestamp,
        sale_price,
        DATE(approximate_arrival_timestamp) AS arrival_date,
        n_records
    FROM
        deduplicate
)
SELECT
    *
FROM
    clean_str
WHERE
    category NOT IN (
        SELECT
            category
        FROM
            dev.silver.product_allowed_categories
    )
    OR n_records > 1
    OR sale_price <= 0