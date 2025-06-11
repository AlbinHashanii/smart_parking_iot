from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

# Initialize Spark
spark = SparkSession.builder.appName("ParkingPrediction").getOrCreate()

# Load historical data
cluster = Cluster(['localhost'])
session = cluster.connect('parking_keyspace')

# Metrics data (free slots)
rows = session.execute("SELECT timestamp, free_slots FROM metrics")
metrics_df = spark.createDataFrame([(row.timestamp, row.free_slots) for row in rows], ["timestamp", "free_slots"])

# Slot metrics (durations, occupancy)
rows = session.execute("SELECT slot_id, avg_duration, timestamp FROM slot_metrics")
slot_df = spark.createDataFrame([(row.slot_id, row.avg_duration, row.timestamp) for row in rows], ["slot_id", "avg_duration", "timestamp"])

# Feature engineering
metrics_df = metrics_df.withColumn("day_of_week", dayofweek(col("timestamp")))
metrics_df = metrics_df.withColumn("hour", hour(col("timestamp")))
slot_df = slot_df.withColumn("day_of_week", dayofweek(col("timestamp")))
slot_df = slot_df.withColumn("hour", hour(col("timestamp")))

# Compute slot occupancy probabilities
occupancy_df = spark.sql("""
    SELECT slot_id, hour, AVG(occupied) as occ_prob
    FROM raw_data
    WHERE timestamp >= current_date - interval '30 days'
    GROUP BY slot_id, hour
""")

# Train model for free slots
assembler = VectorAssembler(inputCols=["day_of_week", "hour"], outputCol="features")
metrics_df = assembler.transform(metrics_df)
rf = RandomForestRegressor(featuresCol="features", labelCol="free_slots", numTrees=20)
model = rf.fit(metrics_df)

# Predict for tomorrow
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
predictions = []
for hour in range(24):
    # Free slots prediction
    future_df = spark.createDataFrame([(datetime.now().weekday() + 1 % 7, hour)], ["day_of_week", "hour"])
    future_df = assembler.transform(future_df)
    free_slots = int(model.predict(future_df.select("features").first().features))

    # Peak timespan
    peak_timespan = "8-10 AM" if 8 <= hour <= 10 else "4-6 PM" if 16 <= hour <= 18 else "Normal"

    # Slot occupancy probabilities
    slot_probs = {}
    for slot_id in [f'S{i+1}' for i in range(50)]:
        prob_row = occupancy_df.filter((col("slot_id") == slot_id) & (col("hour") == hour)).select("occ_prob").collect()
        slot_probs[slot_id] = prob_row[0].occ_prob if prob_row else 0.5

    # Average duration
    avg_duration_row = slot_df.filter(col("hour") == hour).groupBy().avg("avg_duration").collect()
    avg_duration = avg_duration_row[0]["avg(avg_duration)"] if avg_duration_row else 60.0

    predictions.append((tomorrow, hour, free_slots, peak_timespan, slot_probs, avg_duration))

# Save predictions
for date, hour, free_slots, peak_timespan, slot_occupancy, avg_duration in predictions:
    query = "INSERT INTO predictions (date, hour, free_slots, peak_timespan, slot_occupancy, avg_duration) VALUES (%s, %s, %s, %s, %s, %s)"
    session.execute(query, (date, hour, free_slots, peak_timespan, slot_occupancy, avg_duration))

cluster.shutdown()
spark.stop()