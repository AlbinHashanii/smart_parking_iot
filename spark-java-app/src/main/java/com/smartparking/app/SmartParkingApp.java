package com.smartparking.app;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class SmartParkingApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String cassandraHost = System.getenv().getOrDefault("CASSANDRA_HOST", "127.0.0.1");

        SparkSession spark = SparkSession.builder()
            .appName("SmartParkingSparkApp")
            .master("local[*]")
            .config("spark.cassandra.connection.host", cassandraHost)
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> kafkaStream = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("subscribe", "parking-sensor-data")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load();

        // Parse JSON data and keep the original 'json_data' column
        Dataset<Row> parsed = kafkaStream
            .selectExpr("CAST(value AS STRING) AS json_data", "timestamp AS kafka_ingest_time")
            .withColumn("parking_lot_name",
                functions.get_json_object(functions.col("json_data"), "$.parking_lot_name").cast("string"))
            .withColumn("slot_id",
                functions.get_json_object(functions.col("json_data"), "$.slot_id").cast("int"))
            .withColumn("timestamp_from_json",
                functions.get_json_object(functions.col("json_data"), "$.timestamp").cast("timestamp"))
            .withColumn("status",
                functions.get_json_object(functions.col("json_data"), "$.status").cast("string"))
            .withColumn("duration",
                functions.get_json_object(functions.col("json_data"), "$.duration").cast("int"))
            .withColumn("temperature",
                functions.get_json_object(functions.col("json_data"), "$.temperature").cast("int"));

        // Separate out "bad" records (missing parking_lot_name) for inspection
        // Select specific columns for badRecords, including the problematic 'json_data'
        Dataset<Row> badRecords = parsed.filter(functions.col("parking_lot_name").isNull())
                                        .select(functions.col("json_data"), functions.col("kafka_ingest_time")); // KEEP json_data

        // Stream for bad records to console
        badRecords.writeStream()
            .format("console")
            .option("truncate", false)
            .trigger(Trigger.ProcessingTime("30 seconds"))
            .start();

        // Valid records (non-null parking_lot_name)
        Dataset<Row> valid = parsed.filter(functions.col("parking_lot_name").isNotNull());

        // Historical Data Stream (for sensor_data table)
        Dataset<Row> historical = valid
            .withColumn("final_timestamp",
                functions.coalesce(functions.col("timestamp_from_json"), functions.col("kafka_ingest_time")))
            .selectExpr(
                "parking_lot_name",
                "slot_id",
                "final_timestamp AS timestamp",
                "status",
                "duration",
                "temperature"
            );

        // Write historical data to Cassandra
        StreamingQuery histQuery = historical.writeStream()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "parking")
            .option("table", "sensor_data")
            .option("checkpointLocation", "/tmp/spark-checkpoint/sensor_data")
            .outputMode("append")
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .start();

        // Current Status Stream (for parking_spot_current_status table)
        Dataset<Row> currentStatus = valid
            .withColumn("last_updated",
                functions.coalesce(functions.col("timestamp_from_json"), functions.col("kafka_ingest_time")))
            .select("parking_lot_name", "slot_id", "status", "last_updated");

        // Write current status to Cassandra
        StreamingQuery statusQuery = currentStatus.writeStream()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "parking")
            .option("table", "parking_spot_current_status")
            .option("checkpointLocation", "/tmp/spark-checkpoint/current_status")
            .outputMode("append") // Use "append" since it acts as upsert on PK for Cassandra
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .start();

        // Write historical data to console (for debugging)
        historical.writeStream()
            .format("console")
            .option("truncate", false)
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .start();

        histQuery.awaitTermination();
        statusQuery.awaitTermination();
    }
}

// java -cp /home/webintel/Desktop/FIEK_MASTER/SEM2/IoT/smart-parking/sensor-simulator-java/target/sensor-simulator-1.0-SNAPSHOT.jar main.java.com.smartparking.simulator.SensorDataProducer