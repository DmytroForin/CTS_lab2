from flask import Flask, request, jsonify

app = Flask(__name__)
data_store = {}  # {table_name: { (partition_key, sort_key): value }}

@app.route("/register_table", methods=["POST"])
def register_table():
    body = request.json
    table_name = body.get("table_name")
    if not table_name:
        return jsonify({"error": "Missing table_name"}), 400
    if table_name in data_store:
        return jsonify({"error": "Table already exists"}), 400
    data_store[table_name] = {}
    return jsonify({"status": "registered", "table": table_name}), 201

@app.route("/create", methods=["POST"])
def create():
    body = request.json
    table = body.get("table_name")
    pkey = body.get("partition_key")
    skey = body.get("sort_key")
    value = body.get("value")

    if table not in data_store:
        return jsonify({"error": f"Table {table} not found"}), 404

    key = (pkey, skey)
    if key in data_store[table]:
        return jsonify({"error": "Item already exists"}), 400

    data_store[table][key] = value
    return jsonify({"status": "created"}), 201

@app.route("/read/<table>/<partition_key>/<sort_key>", methods=["GET"])
def read(table, partition_key, sort_key):
    if table not in data_store:
        return jsonify({"error": "Table not found"}), 404
    key = (partition_key, sort_key)
    if key not in data_store[table]:
        return jsonify({"error": "Item not found"}), 404
    return jsonify({"table": table, "key": key, "value": data_store[table][key]}), 200

@app.route("/delete/<table>/<partition_key>/<sort_key>", methods=["DELETE"])
def delete(table, partition_key, sort_key):
    if table not in data_store:
        return jsonify({"error": "Table not found"}), 404
    key = (partition_key, sort_key)
    if key in data_store[table]:
        del data_store[table][key]
        return jsonify({"status": "deleted"}), 200
    return jsonify({"error": "Item not found"}), 404

@app.route("/exists/<table>/<partition_key>/<sort_key>", methods=["GET"])
def exists(table, partition_key, sort_key):
    if table not in data_store:
        return jsonify({"exists": False}), 200
    key = (partition_key, sort_key)
    return jsonify({"exists": key in data_store[table]}), 200

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    app.run(port=port)
