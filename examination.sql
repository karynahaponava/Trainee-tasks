-- 1. ГЛАВНАЯ ПРОВЕРКА: Логи (Audit Log)

-- PROCEDURE_NAME: PROCESS_FLIGHTS
-- AFFECTED_ROWS:  98619 (или больше, если дублировала)
-- LOG_TIMESTAMP:  Текущее время (UTC)
SELECT * FROM AIRLINE_DWH.UTILS.AUDIT_LOG 
ORDER BY LOG_TIMESTAMP DESC 
LIMIT 5;


-- 2. ПРОВЕРКА ФАКТОВ (Fact Table)
-- Таблица должна быть заполнена.
SELECT * FROM AIRLINE_DWH.PROCESSED.FACT_FLIGHTS LIMIT 10;

-- Можно посчитать общее количество строк (должно совпадать с логом):
SELECT COUNT(*) FROM AIRLINE_DWH.PROCESSED.FACT_FLIGHTS;


-- 3. ПРОВЕРКА ИЗМЕРЕНИЙ (Dimensions)
-- Здесь должны лежать красивые данные о людях.
SELECT * FROM AIRLINE_DWH.PROCESSED.DIM_PASSENGERS LIMIT 10;

-- Здесь должны лежать данные об аэропортах.
SELECT * FROM AIRLINE_DWH.PROCESSED.DIM_AIRPORTS LIMIT 10;


-- 4. ПРОВЕРКА ОТЧЕТА (Secure View)
-- Этот запрос проверяет, работает ли JOIN между Фактами и Аэропортами.
SELECT * FROM AIRLINE_DWH.PRESENTATION.FLIGHTS_REPORT_VIEW 
ORDER BY TOTAL_FLIGHTS DESC 
LIMIT 10;


-- 5. ПРОВЕРКА СТРИМА (Stream)
-- Стрим должен быть ПУСТ (возвращать FALSE), 
-- потому что процедура уже забрала и обработала все данные.
SELECT SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW.FLIGHTS_STREAM');