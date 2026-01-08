from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Описываем параметры DAG
with DAG(
    dag_id='hello_world_test',           # Уникальное имя, которое будет видно в интерфейсе
    start_date=datetime(2023, 1, 1),     # Дата начала (формальная)
    schedule_interval=None,              # Запуск только вручную (не по расписанию)
    catchup=False                        # Не запускать за прошлые даты
) as dag:

    # Первая задача: просто пишет в лог "Hello"
    task1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Привет! Airflow работает!"'
    )

    # Вторая задача: пишет текущую дату
    task2 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Указываем порядок: сначала task1, потом task2
    task1 >> task2