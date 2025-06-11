from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Initialize Spark
spark = SparkSession.builder.appName("SmartParking").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ssc = StreamingContext(spark.sparkContext, 5)  # 5-second batch interval

# Kafka configuration
kafka_params = {"bootstrap.servers": "localhost:9092", "group.id": "parking_group"}
kafka_topic = "parking_data"

# Read from Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafka_params)

# Parse JSON data
def parse_json(line):
    try:
        data = json.loads(line[1])
        return [(data["sensor_id"], data["slot_id"], data["occupied"], data["timestamp"])]
    except:
        return []

parsed_stream = kafka_stream.flatMap(parse_json)

# Process data: Count free slots
def process_rdd(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd, ["sensor_id", "slot_id", "occupied", "timestamp"])
        # Aggregate free slots
        free_slots = df.filter(df.occupied == False).groupBy().count().collect()[0]["count"]
        
        # Save to Cassandra
        cluster = Cluster(['localhost'])
        session = cluster.connect('parking_keyspace')
        # Insert raw data
        for row in df.collect():
            query = "INSERT INTO raw_data (sensor_id, slot_id, occupied, timestamp) VALUES (%s, %s, %s, %s)"
            session.execute(query, (row.sensor_id, row.slot_id, row.occupied, row.timestamp))
        # Insert metrics
        query = "INSERT INTO metrics (timestamp, free_slots) VALUES (%s, %s)"
        session.execute(query, (df.select("timestamp").first()[0], free_slots))
        cluster.shutdown()

parsed_stream.foreachRDD(process_rdd)

# Start streaming
ssc.start()
ssc.awaitTermination()