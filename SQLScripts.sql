CREATE SCHEMA IF NOT EXISTS stg; 
CREATE SCHEMA IF NOT EXISTS core; 
CREATE SCHEMA IF NOT EXISTS mart;  

DROP TABLE IF EXISTS stg.import_superstore;

CREATE TABLE stg.import_superstore (
    row_id          VARCHAR(50), 
    order_id        VARCHAR(50),
    order_date      VARCHAR(20), 
    ship_date       VARCHAR(20),
    ship_mode       VARCHAR(50),
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(255),
    segment         VARCHAR(50),
    country         VARCHAR(100),
    city            VARCHAR(100),
    state           VARCHAR(100),
    postal_code     VARCHAR(20),
    region          VARCHAR(50),
    product_id      VARCHAR(50),
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    product_name    TEXT,
    sales           VARCHAR(50), 
    quantity        VARCHAR(20),
    discount        VARCHAR(20),
    profit          VARCHAR(50)
);

DROP TABLE IF EXISTS core.dim_customers;
CREATE TABLE core.dim_customers (
    customer_sk     SERIAL PRIMARY KEY,  
    customer_id     VARCHAR(50),         
    customer_name   VARCHAR(255),       
    segment         VARCHAR(50),        
    valid_from      TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    valid_to        TIMESTAMP,          
    is_current      BOOLEAN DEFAULT TRUE 
);

DROP TABLE IF EXISTS core.dim_locations;
CREATE TABLE core.dim_locations (
    location_id     SERIAL PRIMARY KEY,
    country         VARCHAR(100),
    state           VARCHAR(100),
    city            VARCHAR(100),
    postal_code     VARCHAR(20),
    region          VARCHAR(50),
    CONSTRAINT uq_location UNIQUE (country, state, city, postal_code)
);

DROP TABLE IF EXISTS core.dim_products;
CREATE TABLE core.dim_products (
    product_id      VARCHAR(50) PRIMARY KEY,
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    product_name    TEXT
);

DROP TABLE IF EXISTS core.fct_orders;
CREATE TABLE core.fct_orders (
    order_pk        SERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_date      DATE,
    ship_date       DATE,
    ship_mode       VARCHAR(50),
    customer_sk     INT,           
    location_id     INT,           
    product_id      VARCHAR(50),  
    sales           DECIMAL(10,4),
    quantity        INT,
    discount        DECIMAL(4,2),
    profit          DECIMAL(10,4),
    source_row_id   VARCHAR(50)    
);

DROP TABLE IF EXISTS mart.fact_sales;
CREATE TABLE mart.fact_sales (
    sale_key        SERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_date      DATE,
    customer_key    INT,  
    location_key    INT,
    product_key     VARCHAR(50),
    sales_amount    DECIMAL(10,4),
    profit_amount   DECIMAL(10,4),
    quantity        INT
);


/*------------Сырой слой-------------*/
TRUNCATE TABLE stg.import_superstore;

COPY stg.import_superstore (
    row_id, order_id, order_date, ship_date, ship_mode, 
    customer_id, customer_name, segment, country, city, 
    state, postal_code, region, product_id, category, 
    sub_category, product_name, sales, quantity, discount, profit
) 
FROM '/data_import/initial_load.csv' 
DELIMITER ',' 
CSV HEADER;

SELECT count(*) as loaded_rows FROM stg.import_superstore;


/*----------------------ЗАГРУЗКА ИЗ STAGE В CORE----------------------*/

INSERT INTO core.dim_locations (country, state, city, postal_code, region)
SELECT DISTINCT country, state, city, postal_code, region
FROM stg.import_superstore
ON CONFLICT (country, state, city, postal_code) DO NOTHING; 

INSERT INTO core.dim_products (product_id, category, sub_category, product_name)
SELECT DISTINCT product_id, category, sub_category, product_name
FROM stg.import_superstore
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO core.dim_customers (customer_id, customer_name, segment, valid_from, is_current)
SELECT DISTINCT customer_id, customer_name, segment, NOW(), TRUE
FROM stg.import_superstore;

INSERT INTO core.fct_orders (
    order_id, order_date, ship_date, ship_mode, 
    customer_sk, location_id, product_id, 
    sales, quantity, discount, profit, source_row_id
)
SELECT 
    s.order_id,
    TO_DATE(s.order_date, 'MM/DD/YYYY'), 
    TO_DATE(s.ship_date, 'MM/DD/YYYY'),
    s.ship_mode,
    c.customer_sk,    
    l.location_id,    
    s.product_id,
    CAST(s.sales AS DECIMAL(10,4)), 
    CAST(s.quantity AS INT),
    CAST(s.discount AS DECIMAL(4,2)),
    CAST(s.profit AS DECIMAL(10,4)),
    s.row_id
FROM stg.import_superstore s
JOIN core.dim_customers c ON s.customer_id = c.customer_id 
JOIN core.dim_locations l ON 
    s.postal_code = l.postal_code AND 
    s.city = l.city AND 
    s.state = l.state;

SELECT 'Locations' as table_name, count(*) as cnt FROM core.dim_locations
UNION ALL
SELECT 'Products', count(*) FROM core.dim_products
UNION ALL
SELECT 'Customers', count(*) FROM core.dim_customers
UNION ALL
SELECT 'Orders', count(*) FROM core.fct_orders;


/*--------------Очищаем Stage от старых данных --------------------*/
TRUNCATE TABLE stg.import_superstore;

COPY stg.import_superstore (
    row_id, order_id, order_date, ship_date, ship_mode, 
    customer_id, customer_name, segment, country, city, 
    state, postal_code, region, product_id, category, 
    sub_category, product_name, sales, quantity, discount, profit
) 
FROM '/data_import/secondary_load.csv' 
DELIMITER ',' 
CSV HEADER;

SELECT count(*) as secondary_rows FROM stg.import_superstore;


/*-------------ETL ПРОЦЕСС: SECONDARY LOAD--------------*/

INSERT INTO core.dim_locations (country, state, city, postal_code, region)
SELECT DISTINCT country, state, city, postal_code, region FROM stg.import_superstore
ON CONFLICT (country, state, city, postal_code) DO NOTHING;

INSERT INTO core.dim_products (product_id, category, sub_category, product_name)
SELECT DISTINCT product_id, category, sub_category, product_name FROM stg.import_superstore
ON CONFLICT (product_id) DO NOTHING;

UPDATE core.dim_customers c
SET customer_name = s.customer_name
FROM stg.import_superstore s
WHERE c.customer_id = s.customer_id 
  AND c.is_current = TRUE
  AND c.customer_name <> s.customer_name;

INSERT INTO core.dim_customers (customer_id, customer_name, segment, valid_from, is_current)
SELECT DISTINCT s.customer_id, s.customer_name, s.segment, NOW(), TRUE
FROM stg.import_superstore s
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_customers c WHERE c.customer_id = s.customer_id
);

INSERT INTO core.fct_orders (
    order_id, order_date, ship_date, ship_mode, 
    customer_sk, location_id, product_id, 
    sales, quantity, discount, profit, source_row_id
)
SELECT 
    s.order_id,
    TO_DATE(s.order_date, 'MM/DD/YYYY'), 
    TO_DATE(s.ship_date, 'MM/DD/YYYY'),
    s.ship_mode,
    c.customer_sk,
    l.location_id,
    s.product_id,
    CAST(s.sales AS DECIMAL(10,4)),
    CAST(s.quantity AS INT),
    CAST(s.discount AS DECIMAL(4,2)),
    CAST(s.profit AS DECIMAL(10,4)),
    s.row_id
FROM stg.import_superstore s
JOIN core.dim_customers c ON s.customer_id = c.customer_id AND c.is_current = TRUE
JOIN core.dim_locations l ON s.postal_code = l.postal_code AND s.city = l.city
WHERE NOT EXISTS (
    SELECT 1 FROM core.fct_orders f WHERE f.source_row_id = s.row_id
);

/*-------------------проверка----------------*/
SELECT count(*) FROM core.fct_orders;

SELECT customer_id, customer_name 
FROM core.dim_customers 
WHERE customer_name LIKE '%(Updated)%';


/*------------------MART LAYER---------------------*/

-- 1. КАЛЕНДАРЬ (Date Dimension) 
DROP TABLE IF EXISTS mart.dim_calendar;
CREATE TABLE mart.dim_calendar AS
WITH dates AS (
    SELECT GENERATE_SERIES(
        '2014-01-01'::DATE, 
        '2025-12-31'::DATE, 
        '1 day'::INTERVAL
    )::DATE AS date_key
)
SELECT 
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    EXTRACT(MONTH FROM date_key) AS month_num,
    TO_CHAR(date_key, 'Month') AS month_name,
    TO_CHAR(date_key, 'Day') AS day_name,
    EXTRACT(ISODOW FROM date_key) AS day_of_week
FROM dates;

-- 2. ИЗМЕРЕНИЕ: ТОВАРЫ (Denormalized Product)
DROP TABLE IF EXISTS mart.dim_products;
CREATE TABLE mart.dim_products AS
SELECT 
    product_id,
    category,
    sub_category,
    product_name
FROM core.dim_products;

-- 3. ИЗМЕРЕНИЕ: КЛИЕНТЫ И ЛОКАЦИИ (Объединяем для удобства)
DROP TABLE IF EXISTS mart.dim_customers;
CREATE TABLE mart.dim_customers AS
SELECT 
    c.customer_sk,
    c.customer_id,
    c.customer_name,
    c.segment,
    c.is_current 
FROM core.dim_customers c
WHERE c.is_current = TRUE; 

-- 4. ИЗМЕРЕНИЕ: ГЕОГРАФИЯ
DROP TABLE IF EXISTS mart.dim_locations;
CREATE TABLE mart.dim_locations AS
SELECT * FROM core.dim_locations;

-- 5. ТАБЛИЦА ФАКТОВ (Sales Fact)
DROP TABLE IF EXISTS mart.fact_sales;
CREATE TABLE mart.fact_sales AS
SELECT 
    f.order_pk,
    f.order_id,
    f.order_date,    
    f.customer_sk,   
    f.location_id,  
    f.product_id,    
    f.sales,
    f.quantity,
    f.discount,
    f.profit
FROM core.fct_orders f;

ALTER TABLE mart.dim_calendar ADD PRIMARY KEY (date_key);
ALTER TABLE mart.dim_products ADD PRIMARY KEY (product_id);
ALTER TABLE mart.dim_customers ADD PRIMARY KEY (customer_sk);
ALTER TABLE mart.dim_locations ADD PRIMARY KEY (location_id);
ALTER TABLE mart.fact_sales ADD PRIMARY KEY (order_pk);