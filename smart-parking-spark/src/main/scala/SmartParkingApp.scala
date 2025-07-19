import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SmartParkingApp {
  val FAILURE_THRESHOLD_SECONDS = 30

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Smart Parking Kafka to Cassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    // 1. Read Kafka stream
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "parking-sensor-data")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")

    // 2. Define schema
    val schema = new org.apache.spark.sql.types.StructType()
      .add("sensor_id", "string")
      .add("parking_lot_name", "string")
      .add("slot_id", "integer")
      .add("reading_ts", "string")
      .add("status", "string")
      .add("duration", "integer")
      .add("temperature", "double")
      .add("vehicle_license_plate", "string")

    // 3. Parse JSON
    val parsedDF = kafkaDF
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"reading_ts"))
      .drop("reading_ts")
      .withWatermark("timestamp", "1 minute")

    parsedDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val metadata = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "parking", "table" -> "sensor_metadata"))
        .load()
        .select("sensor_id", "slot_id", "parking_lot_id")

      val now = spark.sql("SELECT current_timestamp() as now").first().getTimestamp(0)

      // Join with metadata
      val enriched = batchDF.join(metadata, Seq("sensor_id", "slot_id"), "left")
      val valid = enriched.filter($"sensor_id".isNotNull)

      // Write to sensor_data
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

      // Write to parking_spot_current_status
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

      // === Silent Failure Detection Logic ===
      val allSensors = metadata.select("sensor_id", "slot_id", "parking_lot_id")
      val lastSeen = valid.groupBy("sensor_id", "slot_id", "parking_lot_id")
        .agg(max("timestamp").as("last_seen"))

      val checkFailures = allSensors.join(lastSeen, Seq("sensor_id", "slot_id", "parking_lot_id"), "left_outer")
        .withColumn("now", lit(now))
        .withColumn("seconds_since_last_seen", unix_timestamp($"now") - unix_timestamp($"last_seen"))
        .filter($"last_seen".isNull || $"seconds_since_last_seen" > FAILURE_THRESHOLD_SECONDS)

      // Only write failure state if sensor hasn't been seen for over 30s
      val failureToWrite = checkFailures
        .filter($"seconds_since_last_seen" > FAILURE_THRESHOLD_SECONDS || $"last_seen".isNull)
        .select(
          $"sensor_id",
          $"now".as("last_updated"),
          $"parking_lot_id".as("parking_lot_name"),
          $"slot_id",
          lit("sensor_failure").as("status")
        )

      // Save sensor_failure only if confirmed
      if (!failureToWrite.isEmpty) {
        failureToWrite.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "parking_spot_current_status"))
          .mode("append")
          .save()
      }

      println(s"=== Batch $batchId: Silent sensor failures recorded ===")
      failureToWrite.show(false)

    }.start().awaitTermination()
  }
}
