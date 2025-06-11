from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('parking_keyspace')

# Initialize metadata for 50 slots (5x10 grid)
def init_metadata():
    for i in range(50):
        slot_id = f'S{i+1}'
        sensor_id = slot_id  # 1:1 mapping
        x = (i % 10) * 100  # x-coordinate (0, 100, 200, ..., 900)
        y = (i // 10) * 100  # y-coordinate (0, 100, 200, 300, 400)
        query = "INSERT INTO metadata (sensor_id, slot_id, x, y) VALUES (%s, %s, %s, %s)"
        session.execute(query, (sensor_id, slot_id, x, y))
    print("Metadata initialized")

if __name__ == '__main__':
    init_metadata()
    cluster.shutdown()