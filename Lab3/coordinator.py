import connexion
from flask import jsonify
import requests
import itertools
from hashing import ConsistentHashRing

# Ініціалізація connexion (Swagger)
app = connexion.App(__name__, specification_dir='.')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

# ===========================
#   SHARD CONFIG
# ===========================

shards = {
    0: {
        "leader": "http://leader0:5001",
        "followers": [
            "http://follower0a:5000",
            "http://follower0b:5000"
        ]
    },
    1: {
        "leader": "http://leader1:5002",
        "followers": [
            "http://follower1a:5000",
            "http://follower1b:5000"
        ]
    },
    2: {
        "leader": "http://leader2:5003",
        "followers": [
            "http://follower2a:5000",
            "http://follower2b:5000"
        ]
    }
}

# ===========================
#   CONSISTENT HASH RING
# ===========================
# У кільце додаємо лише leader-и, бо саме вони є "canonical"
ring = ConsistentHashRing()
for shard_id in shards:
    ring.add_node(shard_id)

# ===========================
#   ROUND ROBIN LOAD-BALANCING
# ===========================
# По кожному shard_id буде окремий round-robin ітератор
read_iterators = {}

def get_read_target(shard_id: int) -> str:
    """Повертає наступний endpoint для читання (leader + followers)."""

    if shard_id not in read_iterators:
        endpoints = [shards[shard_id]["leader"]] + shards[shard_id]["followers"]
        read_iterators[shard_id] = itertools.cycle(endpoints)

    return next(read_iterators[shard_id])


# ===========================
#   API ROUTES
# ===========================

# def register_table(body):
#     table_name = body.get("table_name")

#     # Реєстрація таблиці на всіх нодах (лідер + фоловери)
#     for shard in shards.values():
#         all_nodes = [shard["leader"]] + shard["followers"]
#         for node in all_nodes:
#             requests.post(f"{node}/register_table", json={"table_name": table_name})

#     return jsonify({"status": f"Table {table_name} registered on all shards"}), 201

def register_table(body):
    table_name = body.get("table_name")

    # Реєстрація таблиці на всіх нодах (лідер + фоловери)
    for shard in shards.keys():
        leader = shards[shard]["leader"]
        requests.post(f"{leader}/register_table", json={"table_name": table_name})
    return jsonify({"status": f"Table {table_name} registered on all shards"}), 201

# ---------------------------
#       CREATE → only leader
# ---------------------------
def create(body):
    table = body["table_name"]
    pkey = body["partition_key"]
    skey = body["sort_key"]

    compound_key = f"{table}:{pkey}:{skey}"

    # Визначаємо shard_id через консистентне хешування
    shard_id = ring.get_node(compound_key)
    leader = shards[shard_id]["leader"]

    # Всі записи — тільки на лідера
    r = requests.post(f"{leader}/create", json=body)
    return jsonify(r.json()), r.status_code


# ---------------------------
#         READ → LB
# ---------------------------
def read(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    shard_id = ring.get_node(compound_key)

    # Отримуємо наступний endpoint (RR)
    target = get_read_target(shard_id)

    r = requests.get(f"{target}/read/{table_name}/{partition_key}/{sort_key}")
    return jsonify(r.json()), r.status_code


# ---------------------------
#   DELETE → leader + followers
# ---------------------------
# def delete(table_name, partition_key, sort_key):
#     compound_key = f"{table_name}:{partition_key}:{sort_key}"
#     shard_id = ring.get_node(compound_key)

#     shard = shards[shard_id]
#     endpoints = [shard["leader"]] + shard["followers"]

#     # Видаляємо на всіх
#     results = []
#     for node in endpoints:
#         r = requests.delete(f"{node}/delete/{table_name}/{partition_key}/{sort_key}")
#         results.append({"node": node, "status": r.status_code})

#     return jsonify({"results": results}), 200

def delete(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    shard_id = ring.get_node(compound_key)

    leader=shards[shard_id]["leader"]

    # Видаляємо на всіх
    results = []
    r = requests.delete(f"{leader}/delete/{table_name}/{partition_key}/{sort_key}")
    results.append({"node": leader, "status": r.status_code})

    return jsonify({"results": results}), 200

# ---------------------------
#         EXISTS
# ---------------------------
def exists(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    shard_id = ring.get_node(compound_key)

    # Беремо лише для читання — load balancing
    target = get_read_target(shard_id)

    r = requests.get(f"{target}/exists/{table_name}/{partition_key}/{sort_key}")
    return jsonify(r.json()), r.status_code


# ===========================
#   RUN
# ===========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
