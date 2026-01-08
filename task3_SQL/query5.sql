SELECT first_name, last_name, film_count
  FROM (
    SELECT first_name, last_name, film_count, DENSE_RANK() OVER (ORDER BY film_count DESC) AS rnk
      FROM (
        SELECT
            actor.actor_id,
            actor.first_name,
            actor.last_name,
            COUNT(film_actor.film_id) AS film_count
          FROM actor JOIN 
	        (film_actor JOIN 
	          (film_category JOIN category ON film_category.category_id = category.category_id) 
	        ON film_actor.film_id = film_category.film_id) 
	      ON actor.actor_id = film_actor.actor_id
          WHERE category.name = 'Children'
          GROUP BY actor.actor_id
      ) AS actor_film_counts 
  ) AS ranked_actors 
  WHERE rnk <= 3
  ORDER BY film_count DESC, first_name





   
