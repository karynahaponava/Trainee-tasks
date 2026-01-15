import json
import boto3
import csv
import os
import urllib.parse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

s3_client = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=REGION)
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL, region_name=REGION)


def get_target_table(key):
    """
    Определяет, в какую таблицу писать данные, исходя из имени файла.
    """
    if "metrics/daily" in key:
        return "DailyMetrics"
    elif "metrics/departures" in key:
        return "DeparturesData"
    elif "metrics/returns" in key:
        return "ReturnsData"
    return None


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    for record in event["Records"]:
        try:
            body = record["body"]
            try:
                sns_message = json.loads(body)
                if "Message" in sns_message:
                    s3_event = json.loads(sns_message["Message"])
                else:
                    s3_event = sns_message
            except:
                s3_event = json.loads(body)

            if "Records" not in s3_event:
                continue

            for s3_record in s3_event["Records"]:
                bucket_name = s3_record["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(
                    s3_record["s3"]["object"]["key"], encoding="utf-8"
                )

                if "_temporary" in key or "_SUCCESS" in key:
                    continue

                table_name = get_target_table(key)
                if not table_name:
                    logger.warning(f"Skipping unknown file path: {key}")
                    continue

                logger.info(f"Processing file: {key} -> Target Table: {table_name}")
                table = dynamodb.Table(table_name)

                download_path = f"/tmp/{os.path.basename(key)}"
                s3_client.download_file(bucket_name, key, download_path)

                with open(download_path, mode="r", encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile)
                    with table.batch_writer() as batch:
                        count = 0
                        for row in reader:
                            item = {k: v for k, v in row.items() if v}

                            if table_name == "DeparturesData":
                                item["StationName"] = row.get("station_dep", "Unknown")
                                if "station_dep" in item:
                                    del item["station_dep"]

                            if table_name == "ReturnsData":
                                item["StationName"] = row.get("station_ret", "Unknown")
                                if "station_ret" in item:
                                    del item["station_ret"]

                            batch.put_item(Item=item)
                            count += 1

                logger.info(
                    f"Successfully loaded {count} rows from {key} into {table_name}"
                )

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            raise e

    return {"statusCode": 200, "body": json.dumps("Processing complete")}
