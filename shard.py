from flask import Flask, request, jsonify

app = Flask(__name__)
data_store = {}

@app.route("/create", methods=["POST"])
def create():
    body = request.json
    key = body["key"]
    value = body["value"]
    data_store[key] = value
    return jsonify({"status": "OK"}), 201

@app.route("/read/<key>", methods=["GET"])
def read(key):
    if key not in data_store:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"key": key, "value": data_store[key]}), 200

@app.route("/delete/<key>", methods=["DELETE"])
def delete(key):
    if key in data_store:
        del data_store[key]
        return jsonify({"status": "Deleted"}), 200
    return jsonify({"error": "Not found"}), 404

@app.route("/exists/<key>", methods=["GET"])
def exists(key):
    return jsonify({"exists": key in data_store}), 200

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    app.run(port=port)
