import boto3
import json
from flask import Flask, request, jsonify
import os
import botocore
import time
app = Flask(__name__)

# ===========================
# CONFIG
# ===========================
SHARD_ID = int(os.getenv("SHARD_ID", "1"))
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")  # http://minio:9000
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET", "my-bucket")
PORT = int(os.getenv("INTERNAL_PORT", "5001"))

WAL_KEY = f"shard_{SHARD_ID}/wal.jsonl"

s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)




def retry_s3(func, retries=10, delay=2, allow_missing=False):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except botocore.exceptions.EndpointConnectionError:
            print(f"[S3] MinIO not ready, retrying ({attempt}/{retries})...")
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"].get("Code")
            if code in ("NoSuchBucket", "NoSuchKey"):
                if allow_missing and code == "NoSuchKey":
                    return None  # просто повертаємо None, WAL ще немає
                print(f"[S3] {code}, retrying ({attempt}/{retries})...")
            else:
                raise
        time.sleep(delay)
    raise RuntimeError("S3 did not become ready in time")


def ensure_bucket():
    def _create():
        return s3.create_bucket(Bucket=BUCKET)
    try:
        retry_s3(_create)
        print(f"[Leader {SHARD_ID}] Bucket {BUCKET} created")
    except botocore.exceptions.ClientError as e:
        print(f"[Leader {SHARD_ID}] Bucket {BUCKET} already exists")

ensure_bucket()



data_store = {}                  # локальна база
last_offset = 0

# ===========================
# HELPERS
# ===========================
def append_wal(record: dict):
    """Append record to S3 WAL"""
    s3.put_object(
        Bucket=BUCKET,
        Key=WAL_KEY,
        Body=(json.dumps(record) + "\n").encode()
        # ⚠ У реальному продакшн потрібен multi-part append або DynamoDB
    )
    def _put():
        return s3.put_object(
            Bucket=BUCKET,
            Key=WAL_KEY,
            Body=(json.dumps(record) + "\n").encode()
        )
    retry_s3(_put)

def load_wal():
    """Load WAL with retry (safe on empty/minio cold start)."""
    global last_offset

    def _load():
        return s3.get_object(Bucket=BUCKET, Key=WAL_KEY)

    try:
        obj = retry_s3(_load, retries=3, delay=1, allow_missing=True)
        if obj is None:
            print(f"[Leader {SHARD_ID}] WAL empty, starting fresh")
            return

        lines = obj["Body"].read().decode().splitlines()
        for line in lines:
            rec = json.loads(line)
            table, key, value, offset = (
                rec["table"],
                (rec["pkey"], rec["skey"]),
                rec["value"],
                rec["offset"],
            )
            if table not in data_store:
                data_store[table] = {}
            data_store[table][key] = value
            last_offset = max(last_offset, offset)

        print(f"[Leader {SHARD_ID}] WAL loaded, last_offset={last_offset}")

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print(f"[Leader {SHARD_ID}] WAL empty")
        else:
            raise

# ===========================
# API
# ===========================
# @app.route("/register_table", methods=["POST"])
# def register_table():
#     body = request.json
#     table_name = body.get("table_name")
#     if not table_name:
#         return jsonify({"error": "Missing table_name"}), 400
#     if table_name not in data_store:
#         data_store[table_name] = {}
#     return jsonify({"status": f"table {table_name} registered"}), 201

@app.route("/register_table", methods=["POST"])
def register_table():
    global last_offset
    body = request.json
    table_name = body.get("table_name")
    if not table_name:
        return jsonify({"error": "Missing table_name"}), 400
    if table_name not in data_store:
        data_store[table_name] = {}

        # записуємо у WAL
        last_offset += 1
        record = {
            "offset": last_offset,
            "op": "create_table",
            "table": table_name
        }
        append_wal(record)

    return jsonify({"status": f"table {table_name} registered", "offset": last_offset}), 201


@app.route("/create", methods=["POST"])
def create():
    global last_offset
    body = request.json
    table = body["table_name"]
    pkey = body["partition_key"]
    skey = body["sort_key"]
    value = body["value"]

    if table not in data_store:
        return jsonify({"error": "Table not found"}), 404
    key = (pkey, skey)
    if key in data_store[table]:
        return jsonify({"error": "Item already exists"}), 400

    last_offset += 1
    record = {
        "offset": last_offset,
        "table": table,
        "pkey": pkey,
        "skey": skey,
        "value": value
    }

    # записуємо локально
    data_store[table][key] = value
    # append в S3 WAL
    append_wal(record)

    return jsonify({"status": "created", "offset": last_offset}), 201


@app.route("/fetch")
def fetch():
    """Follower запитує записи від from_offset"""
    from_offset = int(request.args.get("from_offset", 1))
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=WAL_KEY)
        lines = obj["Body"].read().decode().splitlines()
        records = []
        for line in lines:
            rec = json.loads(line)
            if rec["offset"] >= from_offset:
                records.append(rec)
        return jsonify(records)
    except s3.exceptions.NoSuchKey:
        return jsonify([])


@app.route("/read/<table>/<pkey>/<skey>")
def read(table, pkey, skey):
    key = (pkey, skey)
    if table in data_store and key in data_store[table]:
        return jsonify({"value": data_store[table][key]}), 200
    return jsonify({"error": "Not found"}), 404


# @app.route("/delete/<table>/<pkey>/<skey>", methods=["DELETE"])
# def delete(table, pkey, skey):
#     key = (pkey, skey)
#     if table in data_store and key in data_store[table]:
#         del data_store[table][key]
#         return jsonify({"status": "deleted"}), 200
#     return jsonify({"error": "Not found"}), 404

@app.route("/delete/<table>/<pkey>/<skey>", methods=["DELETE"])
def delete(table, pkey, skey):
    global last_offset
    key = (pkey, skey)

    if table not in data_store or key not in data_store[table]:
        return jsonify({"error": "Not found"}), 404

    # видаляємо локально
    del data_store[table][key]

    # append у WAL
    last_offset += 1
    record = {
        "offset": last_offset,
        "table": table,
        "pkey": pkey,
        "skey": skey,
        "value": None,         # None означає видалення
        "op": "delete"         # додаємо поле операції
    }
    append_wal(record)

    return jsonify({"status": "deleted", "offset": last_offset}), 200



@app.route("/exists/<table>/<pkey>/<skey>")
def exists(table, pkey, skey):
    key = (pkey, skey)
    exists = table in data_store and key in data_store[table]
    return jsonify({"exists": exists})


if __name__ == "__main__":
    load_wal()
    app.run(host="0.0.0.0", port=PORT)