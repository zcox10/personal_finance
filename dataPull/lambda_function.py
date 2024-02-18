import boto3
from datetime import datetime, timezone
import json
from os import listdir
from os.path import isfile, join
import urllib3

CHUNK_SIZE = 10000


def get_num_records():
    return 100000


def get_data(
    start_user_id, end_user_id, get_path="http://jsonplaceholder.typicode.com/posts"
):
    http = urllib3.PoolManager()
    data = {"userId": None, "id": None, "title": None, "body": None}
    try:
        r = http.request(
            "GET",
            get_path,
            retries=urllib3.util.Retry(3),
            fields={"start_user_id": start_user_id, "end_user_id": end_user_id},
        )
        data = json.loads(r.data.decode("utf8").replace("'", '"'))
    except KeyError as e:
        print(f"Wrong format url {get_path}", e)
    except urllib3.exceptions.MaxRetryError as e:
        print(f"API unavailable at {get_path}", e)
    return data


def download_data(N):
    for i in range(0, N, CHUNK_SIZE):
        data = get_data(i, i + CHUNK_SIZE)
        write_to_local(data, i // CHUNK_SIZE)


def lambda_handler(event, context):
    N = get_num_records()
    download_data(N)

    key = _get_key()
    files = [f for f in listdir(LOCAL_FILE_SYS) if isfile(join(LOCAL_FILE_SYS, f))]
    for f in files:
        s3_client.upload_file(LOCAL_FILE_SYS + "/" + f, S3_BUCKET, key + f)
