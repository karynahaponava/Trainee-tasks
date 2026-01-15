$ContainerName = "task9_localstackproject-localstack-1"

Write-Host "--- STARTING LOCALSTACK SETUP ---" -ForegroundColor Green
Write-Host "Target Container: $ContainerName" -ForegroundColor Cyan

# 1. Create S3 Bucket
Write-Host "1. Creating S3 bucket..."
docker exec $ContainerName awslocal s3 mb s3://helsinki-bikes-data

# 2. Create DynamoDB Tables
Write-Host "2. Creating DynamoDB tables..."
docker exec $ContainerName awslocal dynamodb create-table --table-name DeparturesData --attribute-definitions AttributeName=StationName,AttributeType=S --key-schema AttributeName=StationName,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
docker exec $ContainerName awslocal dynamodb create-table --table-name ReturnsData --attribute-definitions AttributeName=StationName,AttributeType=S --key-schema AttributeName=StationName,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
docker exec $ContainerName awslocal dynamodb create-table --table-name DailyMetrics --attribute-definitions AttributeName=Date,AttributeType=S --key-schema AttributeName=Date,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

# 3. Create SNS and SQS
Write-Host "3. Setting up SNS and SQS..."
docker exec $ContainerName awslocal sns create-topic --name MetricsTopic
docker exec $ContainerName awslocal sqs create-queue --queue-name MetricsQueue
docker exec $ContainerName awslocal sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:MetricsTopic --protocol sqs --notification-endpoint arn:aws:sqs:us-east-1:000000000000:MetricsQueue

# 4. Deploy Lambda
Write-Host "4. Deploying Lambda..."
# Copy zip archive
docker cp scripts/lambda_function.zip "$($ContainerName):/tmp/lambda_function.zip"
# Create function
docker exec $ContainerName awslocal lambda create-function --function-name ProcessMetrics --zip-file fileb:///tmp/lambda_function.zip --handler lambda_function.lambda_handler --runtime python3.9 --role arn:aws:iam::000000000000:role/lambda-role --environment Variables="{AWS_ENDPOINT_URL=http://localstack:4566}" --timeout 60 --memory-size 512
# Map SQS to Lambda
docker exec $ContainerName awslocal lambda create-event-source-mapping --function-name ProcessMetrics --batch-size 1 --event-source-arn arn:aws:sqs:us-east-1:000000000000:MetricsQueue

# 5. S3 Notifications
Write-Host "5. Enabling S3 -> SNS notifications..."
docker cp scripts/notification.json "$($ContainerName):/tmp/notification.json"
docker exec $ContainerName awslocal s3api put-bucket-notification-configuration --bucket helsinki-bikes-data --notification-configuration file:///tmp/notification.json

Write-Host "--- DONE! Infrastructure is ready. ---" -ForegroundColor Green