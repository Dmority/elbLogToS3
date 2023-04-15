import boto3
import gzip
import os
from datetime import datetime
from io import BytesIO
from urllib.parse import unquote_plus

s3 = boto3.client("s3")
logs = boto3.client("logs")


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    log_group = "/test/log"
    log_stream = os.path.basename(key)
    try:
        logs.create_log_stream(logGroupName=log_group,
                               logStreamName=log_stream)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass

    with BytesIO() as data:
        s3.download_fileobj(bucket, key, data)
        data.seek(0)
        with gzip.GzipFile(fileobj=data) as f:
            for line in f:
                line = line.decode("utf-8").strip()
                parts = line.split(" ")
                timestamp = datetime.strptime(
                    parts[1], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000
                logs.put_log_events(
                    logGroupName=log_group,
                    logStreamName=log_stream,
                    logEvents=[
                        {
                            "timestamp": int(timestamp),
                            "message": line
                        }
                    ]
                )
                print("end")
