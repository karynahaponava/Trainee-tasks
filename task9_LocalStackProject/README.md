# AWS Data Pipeline (LocalStack, Airflow, Spark)

Этот проект реализует Data Engineering пайплайн для обработки данных велопроката Хельсинки. Используется эмуляция AWS сервисов через LocalStack.

## Архитектура

1. **Airflow**: Оркестрация задач.
2. **S3 (LocalStack)**: Хранение сырых данных и результатов.
3. **Apache Spark**: Агрегация и расчет метрик.
4. **AWS Lambda**: Event-driven обработка (Trigger S3 -> SNS -> SQS -> Lambda).
5. **DynamoDB**: База данных для аналитики.

## Структура проекта

- `dags/` - Airflow DAGs.
- `scripts/` - Python и Spark скрипты (ETL логика).
- `localstack_data/` - Персистентные данные LocalStack.

## Установка и запуск

### Предварительные требования
- Docker & Docker Compose
- Git

### Инструкция

1. Клонируйте репозиторий:
   ```bash
   git clone <your-repo-url>
   cd Task9LocalStack
   ```

2. Создайте файл .env на основе примера:
```bash
cp .env.example .env
```

3. Запустите инфраструктуру:
```bash
docker-compose up -d --build
```

4. Инициализируйте AWS сервисы (бакеты, таблицы): (Если вы на Windows, используйте PowerShell)
```PowerShell
./init_aws.ps1
```

5. Откройте Airflow UI: Перейдите по адресу http://localhost:8080 (login: admin, password: admin).


### Тестирование

Для запуска тестов:
```bash
pytest tests/
```
## Результаты анализа (Tableau)

В ходе анализа была выявлена корреляция между средней температурой и количеством поездок.
Ниже представлен график зависимости:

![График из Tableau](dashboard_screenshot.png)

*(Если картинка не отображается, см. файл dashboard_screenshot.png в корне репозитория)*