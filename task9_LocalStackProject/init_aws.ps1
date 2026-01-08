Write-Host "--- НАЧАЛО НАСТРОЙКИ LOCALSTACK ---" -ForegroundColor Green

# 1. Создание S3 бакета
Write-Host "1. Создаю S3 бакет..."
docker exec task9localstack-localstack-1 awslocal s3 mb s3://helsinki-bikes-data

# 2. Создание таблиц DynamoDB
Write-Host "2. Создаю таблицы DynamoDB..."
docker exec task9localstack-localstack-1 awslocal dynamodb create-table --table-name DeparturesData --attribute-definitions AttributeName=StationName,AttributeType=S --key-schema AttributeName=StationName,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
docker exec task9localstack-localstack-1 awslocal dynamodb create-table --table-name ReturnsData --attribute-definitions AttributeName=StationName,AttributeType=S --key-schema AttributeName=StationName,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
docker exec task9localstack-localstack-1 awslocal dynamodb create-table --table-name DailyMetrics --attribute-definitions AttributeName=Date,AttributeType=S --key-schema AttributeName=Date,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

# 3. Создание SNS и SQS
Write-Host "3. Настраиваю очереди SNS и SQS..."
docker exec task9localstack-localstack-1 awslocal sns create-topic --name MetricsTopic
docker exec task9localstack-localstack-1 awslocal sqs create-queue --queue-name MetricsQueue
docker exec task9localstack-localstack-1 awslocal sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:MetricsTopic --protocol sqs --notification-endpoint arn:aws:sqs:us-east-1:000000000000:MetricsQueue

# 4. Деплой Лямбды
Write-Host "4. Загружаю и настраиваю Лямбду..."
# Копируем архив внутрь
docker cp scripts/lambda_function.zip task9localstack-localstack-1:/tmp/lambda_function.zip
# Создаем функцию
docker exec task9localstack-localstack-1 awslocal lambda create-function --function-name ProcessMetrics --zip-file fileb:///tmp/lambda_function.zip --handler lambda_function.lambda_handler --runtime python3.9 --role arn:aws:iam::000000000000:role/lambda-role --environment Variables="{AWS_ENDPOINT_URL=http://localstack:4566}" --timeout 60 --memory-size 512
# Привязываем к очереди
docker exec task9localstack-localstack-1 awslocal lambda create-event-source-mapping --function-name ProcessMetrics --batch-size 1 --event-source-arn arn:aws:sqs:us-east-1:000000000000:MetricsQueue

# 5. Настройка S3 уведомлений
Write-Host "5. Включаю уведомления S3 -> SNS..."
docker cp scripts/notification.json task9localstack-localstack-1:/tmp/notification.json
docker exec task9localstack-localstack-1 awslocal s3api put-bucket-notification-configuration --bucket helsinki-bikes-data --notification-configuration file:///tmp/notification.json

Write-Host "--- ГОТОВО! Инфраструктура поднята. ---" -ForegroundColor Green