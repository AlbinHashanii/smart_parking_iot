import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SmartParkingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Smart Parking Kafka to Cassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    // Step 1: Read from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "parking-sensor-data")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")

    // Step 2: Define schema for parsing JSON
    val schema = new org.apache.spark.sql.types.StructType()
      .add("sensor_id", "string")
      .add("parking_lot_name", "string") // only used in input, not join
      .add("slot_id", "integer")
      .add("reading_ts", "string")
      .add("status", "string")
      .add("duration", "integer")
      .add("temperature", "double")
      .add("vehicle_license_plate", "string")

    // Step 3: Parse Kafka data
    val parsedDF = kafkaDF
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"reading_ts"))
      .drop("reading_ts")

    // Step 4: Process each micro-batch with explicit types
    parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Step 5: Load metadata from Cassandra, explicitly select columns
        val sensorMetadata = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "sensor_metadata"))
          .load()
          .select("sensor_id", "slot_id", "parking_lot_id") // make sure slot_id is present

        // Step 6: Enrich batch data with metadata
        val enriched = batchDF
          .join(sensorMetadata, Seq("sensor_id", "slot_id"), "left")

        val valid = enriched.filter($"sensor_id".isNotNull)

        // Step 7: Write to sensor_data table
        valid.select(
          $"sensor_id",
          $"timestamp",
          $"duration",
          $"status",
          $"temperature",
          $"vehicle_license_plate"
        ).write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "sensor_data"))
          .mode("append")
          .save()

        // Step 8: Write to parking_spot_current_status table
        valid.select(
          $"sensor_id",
          $"timestamp".as("last_updated"),
          $"parking_lot_id".as("parking_lot_name"),
          $"slot_id",
          $"status"
        ).write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "parking_spot_current_status"))
          .mode("append")
          .save()
      }
      .start()
      .awaitTermination()
  }
}
