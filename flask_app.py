from flask import Flask, jsonify
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

app = Flask(__name__)

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('parking_keyspace')

@app.route('/slots', methods=['GET'])
def get_slots():
    # Get latest status for each slot
    query = """
        SELECT slot_id, occupied, timestamp
        FROM raw_data
        WHERE sensor_id = %s
        ORDER BY timestamp DESC
        LIMIT 1
    """
    slots = []
    for i in range(1, 51):
        row = session.execute(query, [f'S{i}']).one()
        if row:
            slots.append({'slot_id': row.slot_id, 'occupied': row.occupied, 'timestamp': row.timestamp})
    return jsonify(slots)

@app.route('/predictions', methods=['GET'])
def get_predictions():
    query = "SELECT date, free_slots FROM predictions"
    rows = session.execute(query)
    predictions = [{'date': row.date, 'free_slots': row.free_slots} for row in rows]
    return jsonify(predictions)

@app.route('/metadata', methods=['GET'])
def get_metadata():
    query = "SELECT slot_id, x, y FROM metadata"
    rows = session.execute(query)
    metadata = [{'slot_id': row.slot_id, 'x': row.x, 'y': row.y} for row in rows]
    return jsonify(metadata)

if __name__ == '__main__':
    app.run(debug=True, port=5000)