# In-Game Purchases ETL Pipeline

## Описание проекта
Полноценный Data Engineering проект. Пайплайн эмулирует поток данных о покупках в магазине видеоигр, обрабатывает их и предоставляет доступ для аналитики.

## 🛠 Технический стек
* **Orchestration:** Apache Airflow
* **Processing:** Apache Spark (PySpark)
* **Storage:** PostgreSQL (Data Warehouse), AWS S3 (Data Lake via LocalStack)
* **API:** FastAPI (REST API для доступа к данным)
* **Quality Assurance:** Pytest (Unit Testing)
* **Containerization:** Docker & Docker Compose

## Ключевые особенности
* **Performance:** Оптимизация SQL-запросов с использованием индексов (Indexing) для ускорения агрегации данных.
* **Analytics Ready:** Подготовленная структура данных (Star Schema) для интеграции с BI-инструментами (Power BI / Metabase).
* **Data Quality:** Автоматическое тестирование логики трансформации данных перед загрузкой.

## ⚙️ Как запустить
1.  Клонируйте репозиторий:
    ```bash
    git clone [https://github.com/karynahaponava/in-game-purchases.git](https://github.com/karynahaponava/in-game-purchases.git)
    ```
2.  Создайте файл `.env` с переменными окружения.
3.  Запустите проект:
    ```bash
    docker-compose up --build -d
    ```
4.  Доступ к интерфейсам:
    * Airflow: `http://localhost:8080`
    * API Docs: `http://localhost:8000/docs`