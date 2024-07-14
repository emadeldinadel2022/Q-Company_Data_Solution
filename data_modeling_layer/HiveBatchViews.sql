SET hive.cli.print.header=true;
-------------------------------------------------------------------------

CREATE VIEW most_selling_products_offline AS
SELECT 
    p.product_id AS ProductID,
    p.product_name AS ProductName,
    SUM(ot.units) AS TotalUnitsSold,
    SUM(ot.total_price) AS TotalRevenue
FROM 
    product_dim p
JOIN 
    offline_transactions_fact ot ON p.product_key = ot.product_key
GROUP BY 
    p.product_id, p.product_name
ORDER BY 
    TotalUnitsSold DESC;
------------------------------------------------------------------------------

CREATE VIEW most_selling_products_online AS
SELECT 
    p.product_id AS ProductID,
    p.product_name AS ProductName,
    SUM(ot.units) AS TotalUnitsSold,
    SUM(ot.total_price) AS TotalRevenue
FROM 
    product_dim p
JOIN 
    online_transactions_fact ot ON p.product_key = ot.product_key
GROUP BY 
    p.product_id, p.product_name
ORDER BY 
    TotalUnitsSold DESC;
-----------------------------------------------------------------------------

CREATE VIEW most_redeemed_offers_offline AS
SELECT 
    discount AS Offer, 
    COUNT(*) AS most_redeemed_offers
FROM 
    offline_transactions_fact
GROUP BY  
    discount
ORDER BY 
    most_redeemed_offers DESC;
-------------------------------------------------------------------------------

CREATE VIEW most_redeemed_offers_online AS
SELECT 
    discount AS Offer, 
    COUNT(*) AS most_redeemed_offers
FROM 
    online_transactions_fact
GROUP BY  
    discount
ORDER BY 
    most_redeemed_offers DESC;
----------------------------------------------------------------------------

CREATE VIEW most_redeemed_offers_per_product_online AS
SELECT
    p.product_id AS productid,
    p.product_name AS productname,
    onf.discount AS offers,
    COUNT(*) AS most_redeemed_offers_per_product
FROM
    product_dim p
JOIN
    online_transactions_fact onf ON p.product_key = onf.product_key
WHERE
    onf.discount > 0.0
GROUP BY
    p.product_id, p.product_name, onf.discount
ORDER BY
    most_redeemed_offers_per_product DESC;
------------------------------------------------------------------

CREATE VIEW most_redeemed_offers_per_product_offline AS
SELECT
    p.product_id AS productid,
    p.product_name AS productname,
    off.discount AS offers,
    COUNT(*) AS most_redeemed_offers_per_product
FROM
    product_dim p
JOIN
    offline_transactions_fact off ON p.product_key = off.product_key
WHERE
    onf.discount > 0.0
GROUP BY
    p.product_id, p.product_name, off.discount
ORDER BY
    most_redeemed_offers_per_product DESC;

-------------------------------------------------------------------

CREATE VIEW lowest_sales_cities_online AS
SELECT
    onf.shipping_city AS city,
    SUM(onf.total_price) AS total_sales
FROM
    online_transactions_fact onf
GROUP BY
    onf.shipping_city
ORDER BY
    total_sales ASC
LIMIT 10;
-------------------------------------------------------------------

CREATE VIEW lowest_sales_cities_offline AS
SELECT
    off.shipping_city AS city,
    SUM(off.total_price) AS total_sales
FROM
    offline_transactions_fact off
GROUP BY
    off.shipping_city
ORDER BY
    total_sales ASC
LIMIT 10;
-------------------------------------------------------------------