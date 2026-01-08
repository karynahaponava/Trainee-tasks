import boto3
import json
import random
import time
from faker import Faker
from datetime import datetime

S3_ENDPOINT = 'http://localstack:4566'
BUCKET_NAME = 'gcomm-raw-data'
AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'

fake = Faker()

GAMES = ['g_001', 'g_002', 'g_003']
PRODUCTS = [
    {'id': 'p_001', 'price': 15.00, 'name': 'Nike Air Max Virtual'},
    {'id': 'p_002', 'price': 25.50, 'name': 'Adidas Digital Skin'},
    {'id': 'p_003', 'price': 5.00,  'name': 'Health Potion'},
    {'id': 'p_004', 'price': 100.00,'name': 'Golden Sword'}
]

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='us-east-1'
    )

def create_bucket_if_not_exists(s3):
    """Создаем бакет (папку), если его нет"""
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Бакет {BUCKET_NAME} готов.")
    except Exception as e:
        pass

def generate_event():
    """Создает одно событие покупки"""
    product = random.choice(PRODUCTS)
    
    event = {
        "event_id": fake.uuid4(),
        "event_type": "PURCHASE",
        "user_id": fake.uuid4(),           
        "game_id": random.choice(GAMES),   
        "product_id": product['id'],
        "product_name": product['name'],
        "price": product['price'],
        "quantity": random.randint(1, 3),
        "timestamp": datetime.now().isoformat(),
        "user_country": fake.country_code()
    }
    return event

def main():
    print("Запуск генератора данных In-Game Commerce...")
    s3 = get_s3_client()
    create_bucket_if_not_exists(s3)

    for i in range(1, 11): 
        batch_events = [generate_event() for _ in range(5)]
        
        file_content = json.dumps(batch_events)
        
        file_name = f"raw/events_{int(time.time())}_{i}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=file_content
        )
        
        print(f"[Batch {i}] Отправлен файл {file_name} в S3")
        time.sleep(1) 

    print("Генерация завершена.")

if __name__ == "__main__":
    main()