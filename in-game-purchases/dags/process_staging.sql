-- 1. Обновляем справочники (Dimensions)
INSERT INTO storefront.dim_games (game_id, title)
SELECT DISTINCT game_id, 'Unknown Game' FROM storefront.stg_purchases
ON CONFLICT (game_id) DO NOTHING;

INSERT INTO storefront.dim_products (product_id, name, price)
SELECT DISTINCT product_id, 'Unknown Product', 0.0 FROM storefront.stg_purchases
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO storefront.dim_users (user_id, country)
SELECT DISTINCT user_id, 'Unknown' FROM storefront.stg_purchases
ON CONFLICT (user_id) DO NOTHING;

-- 2. Переносим факты (Facts)
INSERT INTO storefront.fact_purchases 
(purchase_id, user_id, game_id, product_id, purchase_timestamp, quantity, total_amount)
SELECT 
    purchase_id, user_id, game_id, product_id, purchase_timestamp, quantity, total_amount
FROM storefront.stg_purchases
ON CONFLICT (purchase_id) DO NOTHING;

-- 3. Очищаем Staging
TRUNCATE TABLE storefront.stg_purchases;