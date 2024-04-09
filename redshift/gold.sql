-- aggragate prices by ingest hour
CREATE VIEW dev.gold.hourly_prices AS
SELECT
    date_trunc(
        'hour',
        approximate_arrival_timestamp + interval '30 minute'
    ) AS hour,
    item,
    item_category,
    vendor,
    MIN(sale_price) AS min_sale_price,
    AVG(sale_price) AS avg_sale_price,
    MAX(sale_price) AS max_sale_price,
    SUM(
        CASE
            WHEN stock_status = 'In Stock' THEN 1
            ELSE 0
        END
    ) AS n_in_stock,
    SUM(
        CASE
            WHEN stock_status = 'Low Stock' THEN 1
            ELSE 0
        END
    ) AS n_low_stock,
    SUM(
        CASE
            WHEN stock_status = 'Out of Stock' THEN 1
            ELSE 0
        END
    ) AS n_out_of_stock
FROM
    dev.silver.temus_product_cleaned
GROUP BY
    date_trunc(
        'hour',
        approximate_arrival_timestamp + interval '30 minute'
    ),
    item,
    item_category,
    vendor;

-- summarise stock status by vendor
CREATE VIEW dev.gold.vendor_stock_status AS
SELECT
    date_trunc(
        'hour',
        approximate_arrival_timestamp + interval '30 minute'
    ) AS hour,
    vendor,
    item_category,
    SUM(
        CASE
            WHEN stock_status = 'In Stock' THEN 1
            ELSE 0
        END
    ) AS n_in_stock,
    SUM(
        CASE
            WHEN stock_status = 'Low Stock' THEN 1
            ELSE 0
        END
    ) AS n_low_stock,
    SUM(
        CASE
            WHEN stock_status = 'Out of Stock' THEN 1
            ELSE 0
        END
    ) AS n_out_of_stock
FROM
    dev.silver.temus_product_cleaned
GROUP BY
    date_trunc(
        'hour',
        approximate_arrival_timestamp + interval '30 minute'
    ),
    vendor,
    item_category;

-- price evolution by item
CREATE VIEW dev.gold.price_evolution AS
SELECT
    approximate_arrival_timestamp,
    item,
    item_category,
    MIN(sale_price) AS min_sale_price,
    AVG(sale_price) AS avg_sale_price,
    MAX(sale_price) AS max_sale_price
FROM
    dev.silver.temus_product_cleaned
GROUP BY
    approximate_arrival_timestamp,
    item,
    item_category;