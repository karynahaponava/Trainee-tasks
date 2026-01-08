WITH grouped_data AS (
  SELECT category.name AS category_name, CASE 
    WHEN city.city LIKE 'a%' THEN 'Starts with a'
    WHEN city.city LIKE '%-%' THEN 'Contains hyphen'
  END AS city_group,
  SUM(film.length) / 60.0 AS total_hours
    FROM (category JOIN (film_category JOIN (film JOIN (inventory JOIN (rental JOIN (customer JOIN (address JOIN city ON address.city_id = city.city_id)
                                                                                     ON customer.address_id = address.address_id)
                                                                        ON rental.customer_id = customer.customer_id)
                                                        ON inventory.inventory_id = rental.inventory_id)
                                            ON film.film_id = inventory.film_id)
                         ON film_category.film_id = film.film_id)
    ON category.category_id = film_category.category_id)
    WHERE city.city LIKE 'a%' OR city.city LIKE '%-%'
	GROUP BY city_group, category_name
), 
ranked_data AS (
  SELECT city_group, category_name, total_hours, DENSE_RANK() OVER (PARTITION BY city_group ORDER BY total_hours DESC) AS rnk
    FROM grouped_data
)

  SELECT city_group, category_name, total_hours
    FROM ranked_data
    WHERE rnk = 1
    ORDER BY city_group