import connexion
from flask import jsonify
import requests
from hashing import ConsistentHashRing

# Ініціалізація connexion app для авто-валідації запитів
app = connexion.App(__name__, specification_dir='.')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)  # прив’язує openapi.yaml

# ініціалізація Flask app depricated
#flask_app = app.app

# Ініціалізація хеш-кільця
ring = ConsistentHashRing()
nodes = ["http://localhost:5001", "http://localhost:5002", "http://localhost:5003"]
for n in nodes:
    ring.add_node(n)


def create(body): # create request
    try:
        key = body["key"]
        value = body["value"]
        node = ring.get_node(key)
        r = requests.post(f"{node}/create", json={"key": key, "value": value})
        return jsonify(r.json()), r.status_code
    except KeyError:
        return {"error": "Missing key or value"}, 400
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, 500


def read(key): #read request
    try:
        node = ring.get_node(key)
        r = requests.get(f"{node}/read/{key}")
        return jsonify(r.json()), r.status_code
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, 500


def delete(key): # delete request
    try:
        node = ring.get_node(key)
        r = requests.delete(f"{node}/delete/{key}")
        return jsonify(r.json()), r.status_code
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, 500


def exists(key): #exists requests
    try:
        node = ring.get_node(key)
        r = requests.get(f"{node}/exists/{key}")
        return jsonify(r.json()), r.status_code
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
