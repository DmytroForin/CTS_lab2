import requests
import threading
import time
from flask import Flask, jsonify, request
import os

LEADER_URL = os.getenv("LEADER_URL", "http://leader0:5001")
SYNC_INTERVAL = 0.5                  # 50ms

app = Flask(__name__)
data_store = {}
last_offset = 0


# def sync_loop():
#     global last_offset
#     while True:
#         try:
#             r = requests.get(f"{LEADER_URL}/fetch?from_offset={last_offset + 1}")
#             records = r.json()
#             for rec in records:
#                 table, key, value, offset = rec["table"], (rec["pkey"], rec["skey"]), rec["value"], rec["offset"]
#                 if table not in data_store:
#                     data_store[table] = {}
#                 data_store[table][key] = value
#                 last_offset = max(last_offset, offset)
#         except Exception as e:
#             pass
#         time.sleep(SYNC_INTERVAL)
# def sync_loop():
#     global last_offset
#     while True:
#         try:
#             r = requests.get(f"{LEADER_URL}/fetch?from_offset={last_offset + 1}")
#             records = r.json()
#             for rec in records:
#                 table, key, offset = rec["table"], (rec["pkey"], rec["skey"]), rec["offset"]
#                 op = rec.get("op", "create")       # дефолт create
#                 if table not in data_store:
#                     data_store[table] = {}

#                 if op == "create":
#                     value = rec["value"]
#                     data_store[table][key] = value
#                 elif op == "delete":
#                     if key in data_store[table]:
#                         del data_store[table][key]

#                 last_offset = max(last_offset, offset)
#         except Exception as e:
#             pass
#         time.sleep(SYNC_INTERVAL)

def sync_loop():
    global last_offset
    while True:
        try:
            r = requests.get(f"{LEADER_URL}/fetch?from_offset={last_offset + 1}")
            records = r.json()
            for rec in records:
                op = rec.get("op", "create")
                table = rec.get("table")
                key = (rec.get("pkey"), rec.get("skey"))
                offset = rec.get("offset")

                if op == "create_table":
                    if table not in data_store:
                        data_store[table] = {}

                elif op == "create":
                    value = rec["value"]
                    if table not in data_store:
                        data_store[table] = {}
                    data_store[table][key] = value

                elif op == "delete":
                    if table in data_store and key in data_store[table]:
                        del data_store[table][key]

                last_offset = max(last_offset, offset)

        except Exception as e:
            pass
        time.sleep(SYNC_INTERVAL)



@app.route("/read/<table>/<pkey>/<skey>")
def read(table, pkey, skey):
    key = (pkey, skey)
    if table in data_store and key in data_store[table]:
        return jsonify({"value": data_store[table][key]}), 200
    return jsonify({"error": "Not found"}), 404


@app.route("/exists/<table>/<pkey>/<skey>")
def exists(table, pkey, skey):
    key = (pkey, skey)
    exists = table in data_store and key in data_store[table]
    return jsonify({"exists": exists})


if __name__ == "__main__":
    # стартуємо фоновий потік синхронізації
    t = threading.Thread(target=sync_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)