CREATE SCHEMA IF NOT EXISTS storefront;

CREATE TABLE IF NOT EXISTS storefront.dim_users (
    user_id VARCHAR(50) PRIMARY KEY,
    country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS storefront.dim_games (
    game_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS storefront.dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS storefront.fact_purchases (
    purchase_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES storefront.dim_users(user_id),
    game_id VARCHAR(50) REFERENCES storefront.dim_games(game_id),
    product_id VARCHAR(50) REFERENCES storefront.dim_products(product_id),
    purchase_timestamp TIMESTAMP,
    quantity INT,
    total_amount DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS storefront.stg_purchases (
    purchase_id VARCHAR(50),
    user_id VARCHAR(50),
    game_id VARCHAR(50),
    product_id VARCHAR(50),
    purchase_timestamp TIMESTAMP,
    quantity INT,
    total_amount DECIMAL(10, 2)
);

INSERT INTO storefront.dim_games (game_id, title) VALUES
('g_001', 'Minecraft'),
('g_002', 'GTA V'),
('g_003', 'Counter-Strike 2')
ON CONFLICT (game_id) DO NOTHING;

INSERT INTO storefront.dim_products (product_id, name, price) VALUES
('p_001', '100 Coins', 1.99),
('p_002', 'Battle Pass', 9.99),
('p_003', 'Skin Pack', 4.99),
('p_004', 'Premium Account', 15.00)
ON CONFLICT (product_id) DO NOTHING;
