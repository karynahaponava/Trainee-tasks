-- =========================================================
-- ЗАДАНИЕ: TIME-TRAVEL (Машина времени)
-- Требование: 2 DDL и 2 DML запроса
-- ВАЖНО: Выполняйте эти шаги ПО ОЧЕРЕДИ.
-- =========================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRLINE_DWH;
USE SCHEMA PROCESSED;

-- Шаг 0: Убеждаемся, что данные есть сейчас
SELECT COUNT(*) FROM FACT_FLIGHTS; 
-- Должно быть > 0.


-- Шаг 1: Имитируем удаление (DML)
-- удаляем все полеты за 2022 год
DELETE FROM FACT_FLIGHTS WHERE DEPARTURE_DATE >= '2022-01-01';

-- Проверяем - данные исчезли
SELECT COUNT(*) FROM FACT_FLIGHTS;


-- Шаг 2: (DML - Запрос 1)
-- Смотрим состояние таблицы 2 минуты назад 
-- offset в секундах (60*2 = 120 секунд)
SELECT count(*) FROM FACT_FLIGHTS AT(OFFSET => -60*2);
-- Результат: число строк, которое было ДО удаления.


-- Шаг 3: Восстановление данных (DML - Запрос 2)
-- Вставляем обратно то, что было потеряно, из прошлого
INSERT INTO FACT_FLIGHTS
SELECT * FROM FACT_FLIGHTS AT(OFFSET => -60*2)
WHERE DEPARTURE_DATE >= '2022-01-01'; -- Восстанавливаем только удаленное

-- Проверяем 
SELECT COUNT(*) FROM FACT_FLIGHTS;


-- =========================================================
-- ЧАСТЬ 2: DDL (Работа со структурой)
-- =========================================================

-- Запрос 3: "Клонирование таблицы" (CLONE)
-- Создаем копию таблицы для тестов на основе состояния 5 минут назад
CREATE OR REPLACE TABLE FACT_FLIGHTS_BACKUP 
CLONE FACT_FLIGHTS AT(OFFSET => -60*5);


-- Запрос 4: "Восстановление удаленной таблицы" (UNDROP)
-- Удаляем таблицу полностью
DROP TABLE FACT_FLIGHTS_BACKUP;

-- Восстанавливаем её из корзины (Undrop)
UNDROP TABLE FACT_FLIGHTS_BACKUP;

-- Проверка
SELECT COUNT(*) FROM FACT_FLIGHTS_BACKUP;