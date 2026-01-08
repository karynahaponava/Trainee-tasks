import json
import boto3
import os
import io
import pandas as pd
from datetime import datetime

AWS_ENDPOINT = os.environ.get('AWS_ENDPOINT_URL', 'http://localstack:4566')
s3_client = boto3.client('s3', endpoint_url=AWS_ENDPOINT)
dynamodb = boto3.resource('dynamodb', endpoint_url=AWS_ENDPOINT)

def lambda_handler(event, context):
    print("Lambda started via SQS/SNS...")
    
    try:
        for record in event['Records']:
            sns_body = json.loads(record['body'])
            s3_msg = json.loads(sns_body['Message'])
            
            if 'Records' not in s3_msg:
                print("Skipping non-S3 message")
                continue
                
            s3_record = s3_msg['Records'][0]
            bucket_name = s3_record['s3']['bucket']['name']
            file_key = s3_record['s3']['object']['key']
            
            print(f"Processing file: {file_key}")

            if "departures_agg" in file_key:
                table_name = "DeparturesData"
                mapping = lambda row: {
                    'StationName': str(row['departure_name']),
                    'Count': int(row['count'])
                }
            elif "returns_agg" in file_key:
                table_name = "ReturnsData"
                mapping = lambda row: {
                    'StationName': str(row['return_name']),
                    'Count': int(row['count'])
                }
            elif "daily_metrics" in file_key:
                table_name = "DailyMetrics"
                mapping = lambda row: {
                    'Date': str(row['Date']),
                    'AvgDistance': str(round(float(row['AvgDistance']), 2)),
                    'AvgDuration': str(round(float(row['AvgDuration']), 2)),
                    'AvgSpeed': str(round(float(row['AvgSpeedKmH']), 2)),
                    'AvgTemp': str(round(float(row['AvgTemp']), 1))
                }
            else:
                print("Not a target file, skipping.")
                continue

            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            
            print(f"Dataframe shape: {df.shape}")
            
            table = dynamodb.Table(table_name)
            
            with table.batch_writer() as batch:
                for index, row in df.iterrows():
                    try:
                        item = mapping(row)
                        batch.put_item(Item=item)
                    except Exception as row_err:
                        print(f"Error processing row {index}: {row_err}")
            
            print(f"Saved to DynamoDB table: {table_name}")

        return {'statusCode': 200, 'body': 'Success'}

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {'statusCode': 200, 'body': str(e)}