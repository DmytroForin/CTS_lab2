import connexion
from flask import jsonify
import requests
from hashing import ConsistentHashRing

# Ініціалізація connexion для автоматичної верифікації запитів
app = connexion.App(__name__, specification_dir='.')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

# Ініціалізація хеш-кільця
ring = ConsistentHashRing()
nodes = ["http://localhost:5001", "http://localhost:5002", "http://localhost:5003"]
for n in nodes:
    ring.add_node(n)

# API-методи

def register_table(body):
    table_name = body.get("table_name")
    for node in nodes:
        requests.post(f"{node}/register_table", json={"table_name": table_name})
    return jsonify({"status": f"Table {table_name} registered on all shards"}), 201

def create(body):
    table = body["table_name"]
    pkey = body["partition_key"]
    skey = body["sort_key"]
    value = body["value"]

    compound_key = f"{table}:{pkey}:{skey}"
    node = ring.get_node(compound_key)
    r = requests.post(f"{node}/create", json=body)
    return jsonify(r.json()), r.status_code

def read(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    node = ring.get_node(compound_key)
    r = requests.get(f"{node}/read/{table_name}/{partition_key}/{sort_key}")
    return jsonify(r.json()), r.status_code

def delete(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    node = ring.get_node(compound_key)
    r = requests.delete(f"{node}/delete/{table_name}/{partition_key}/{sort_key}")
    return jsonify(r.json()), r.status_code

def exists(table_name, partition_key, sort_key):
    compound_key = f"{table_name}:{partition_key}:{sort_key}"
    node = ring.get_node(compound_key)
    r = requests.get(f"{node}/exists/{table_name}/{partition_key}/{sort_key}")
    return jsonify(r.json()), r.status_code

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
