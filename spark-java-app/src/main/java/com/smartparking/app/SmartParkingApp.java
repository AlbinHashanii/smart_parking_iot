package com.smartparking.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import java.util.concurrent.TimeoutException;

public class SmartParkingApp {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // 1. Initialize SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SmartParkingSparkApp")
                .master("spark://spark-master:7077")
                .config("spark.cassandra.connection.host", "cassandra")
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 2. Read Streaming Data from Kafka
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "parking-sensor-data")
                .load();

        Dataset<Row> rawSensorData = kafkaStream.selectExpr("CAST(value AS STRING) as json_data", "timestamp as kafka_ingest_time");

        Dataset<Row> parsedSensorData = rawSensorData
            .withColumn("parking_lot_name", functions.get_json_object(functions.col("json_data"), "$.parking_lot_name").cast("string"))
            .withColumn("slot_id", functions.get_json_object(functions.col("json_data"), "$.slot_id").cast("int"))
            .withColumn("timestamp_from_json", functions.get_json_object(functions.col("json_data"), "$.timestamp").cast("timestamp"))
            .withColumn("status", functions.get_json_object(functions.col("json_data"), "$.status").cast("string"))
            .withColumn("duration", functions.get_json_object(functions.col("json_data"), "$.duration").cast("int"))
            .withColumn("temperature", functions.get_json_object(functions.col("json_data"), "$.temperature").cast("int"))
            .select("parking_lot_name", "slot_id", "timestamp_from_json", "kafka_ingest_time", "status", "duration", "temperature");

        // Prepare data for sensor_data table (historical)
        Dataset<Row> sensorDataToWrite = parsedSensorData
            .withColumn("final_timestamp", functions.coalesce(functions.col("timestamp_from_json"), functions.col("kafka_ingest_time")))
            .select("parking_lot_name", "slot_id", "final_timestamp", "status", "duration", "temperature")
            .withColumnRenamed("final_timestamp", "timestamp");

        // 3. Process and Write to Cassandra (sensor_data table - historical)
        StreamingQuery sensorDataQuery = sensorDataToWrite
                .writeStream()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "parking")
                .option("table", "sensor_data")
                .option("checkpointLocation", "/tmp/spark-checkpoint/sensor_data")
                .outputMode("append") // Only new rows are added to the result table
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start();

        // Prepare data for parking_spot_current_status table (UPSERT)
        Dataset<Row> currentStatusToWrite = parsedSensorData
                .withColumn("last_updated", functions.coalesce(functions.col("timestamp_from_json"), functions.col("kafka_ingest_time")))
                .select("parking_lot_name", "slot_id", "status", "last_updated");

        // 4. Process and Write to Cassandra (parking_spot_current_status table - UPSERT)
        StreamingQuery currentStatusQuery = currentStatusToWrite
                .writeStream()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "parking")
                .option("table", "parking_spot_current_status")
                .option("checkpointLocation", "/tmp/spark-checkpoint/current_status")
                .outputMode("update") // Only rows that are updated in the result table are written.
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start();

        // 5. Wait for the termination of the queries
        sensorDataQuery.awaitTermination();
        currentStatusQuery.awaitTermination();
    }
}
