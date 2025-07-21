import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.Window
import scala.concurrent.duration._
import java.sql.Timestamp

object SmartParkingApp {
  // Application Constants and Anomaly Thresholds
  val FAILURE_THRESHOLD_SECONDS = 20 // For "sensor_failure" (no reporting)
  val MIN_VALID_DURATION = 0
  val MAX_VALID_DURATION = 86400 * 3 // 3 days
  val MIN_VALID_TEMPERATURE = -50.0
  val MAX_VALID_TEMPERATURE = 100.0
  val Z_SCORE_THRESHOLD = 3.0 // Used for both Z-score on values AND Z-score on rates
  val ANOMALY_LOOKBACK_SECONDS_STATS = 3600 * 24 // 24 hours for standard deviation calculations

  val MAX_DURATION_FOR_FREE_STATUS_STUCK = 3600 * 4 // 4 hours for "free" status stuck
  val MIN_DURATION_FOR_OCCUPIED_STATUS_STUCK = 3600 * 24 * 7 // 7 days for "occupied" status stuck

  // Constants for the "Contextual Pattern Anomaly" (Simulated AI)
  val SHORT_OCCUPIED_DURATION_THRESHOLD = 60 // e.g., less than 1 minute
  val HIGH_TEMPERATURE_THRESHOLD = 40.0 // e.g., above 40 degrees Celsius

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Smart Parking Kafka to Cassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.sql.caseSensitive", "true")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("Using Contextual Pattern Anomaly Detection (Simulated AI) and Adaptive Rate of Change Anomaly.")
    println("Robust Sensor Failure Detection implemented.")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "parking-sensor-data")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")

    val schema = new org.apache.spark.sql.types.StructType()
      .add("sensor_id", "string")
      .add("parking_lot_name", "string")
      .add("slot_id", "integer")
      .add("reading_ts", "string")
      .add("status", "string")
      .add("duration", "integer")
      .add("temperature", "double")
      .add("vehicle_license_plate", "string")

    val parsedDF = kafkaDF
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"reading_ts"))
      .drop("reading_ts")
      .withWatermark("timestamp", "5 minutes")

    parsedDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val now = spark.sql("SELECT current_timestamp() as now").first().getTimestamp(0)

      // Phase 1: Process and update statuses for sensors that ARE reporting in this batch
      if (!batchDF.isEmpty) {
        // Read sensor metadata from Cassandra
        val metadata = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "sensor_metadata"))
          .load()
          .select("sensor_id", "slot_id", "parking_lot_id")

        // Join with metadata and filter only valid data
        val initialValidData = batchDF.filter($"sensor_id".isNotNull)
        val enriched = initialValidData.join(metadata, Seq("sensor_id", "slot_id"), "left")
        val validBatchData = enriched.filter($"parking_lot_id".isNotNull)

        if (!validBatchData.isEmpty) {
          // --- Calculate anomaly flags for CURRENTLY REPORTING SENSORS ---

          // 1. Physical Value Anomaly
          val physicalAnomalyTemp = validBatchData.withColumn(
            "is_physical_value_anomaly",
            when(
              ($"duration" < MIN_VALID_DURATION || $"duration" > MAX_VALID_DURATION || $"duration".isNull) ||
              ($"temperature" < MIN_VALID_TEMPERATURE || $"temperature" > MAX_VALID_TEMPERATURE || $"temperature".isNull),
              true
            ).otherwise(false)
          )

          // 2. Z-score Anomaly (for values)
          val windowSpecForZScore = Window.partitionBy("sensor_id")
            .orderBy(col("timestamp").cast("long"))
            .rangeBetween(-ANOMALY_LOOKBACK_SECONDS_STATS, Window.currentRow)

          val zScoreStatsTemp = physicalAnomalyTemp
            .withColumn("avg_temp", avg($"temperature").over(windowSpecForZScore))
            .withColumn("stddev_temp", stddev($"temperature").over(windowSpecForZScore))
            .withColumn("avg_duration", avg($"duration").over(windowSpecForZScore))
            .withColumn("stddev_duration", stddev($"duration").over(windowSpecForZScore))

          val zScoreTemp = zScoreStatsTemp.withColumn(
            "is_z_score_anomaly",
            when(
              (abs(($"temperature" - $"avg_temp") / $"stddev_temp") > Z_SCORE_THRESHOLD && $"stddev_temp" =!= 0) ||
              (abs(($"duration" - $"avg_duration") / $"stddev_duration") > Z_SCORE_THRESHOLD && $"stddev_duration" =!= 0),
              true
            ).otherwise(false)
          ).drop("avg_temp", "stddev_temp", "avg_duration", "stddev_duration")

          // 3. Adaptive Rate of Change Anomaly
          val windowSpecForLag = Window.partitionBy("sensor_id").orderBy("timestamp")

          val rawRatesTemp = zScoreTemp
            .withColumn("prev_temp", lag($"temperature", 1).over(windowSpecForLag))
            .withColumn("prev_duration", lag($"duration", 1).over(windowSpecForLag))
            .withColumn("prev_timestamp", lag($"timestamp", 1).over(windowSpecForLag))
            .withColumn("time_diff_minutes", (unix_timestamp($"timestamp") - unix_timestamp($"prev_timestamp")) / 60.0)
            .withColumn("temp_change_rate_per_minute",
              when($"time_diff_minutes" > 0, abs(($"temperature" - $"prev_temp") / $"time_diff_minutes")).otherwise(0.0)
            )
            .withColumn("duration_change_percent",
              when($"prev_duration".isNotNull && $"prev_duration" =!= 0, abs(($"duration" - $"prev_duration").cast("double") / $"prev_duration".cast("double"))).otherwise(0.0)
            )

          val windowSpecForRateStats = Window.partitionBy("sensor_id")
            .orderBy(col("timestamp").cast("long"))
            .rangeBetween(-ANOMALY_LOOKBACK_SECONDS_STATS, Window.currentRow)

          val rateStatsTemp = rawRatesTemp
            .withColumn("avg_temp_rate", avg($"temp_change_rate_per_minute").over(windowSpecForRateStats))
            .withColumn("stddev_temp_rate", stddev($"temp_change_rate_per_minute").over(windowSpecForRateStats))
            .withColumn("avg_duration_percent_rate", avg($"duration_change_percent").over(windowSpecForRateStats))
            .withColumn("stddev_duration_percent_rate", stddev($"duration_change_percent").over(windowSpecForRateStats))

          val rateChangeTemp = rateStatsTemp.withColumn(
            "is_rate_of_change_anomaly",
            when(
              (abs(($"temp_change_rate_per_minute" - $"avg_temp_rate") / $"stddev_temp_rate") > Z_SCORE_THRESHOLD && $"stddev_temp_rate" =!= 0) ||
              (abs(($"duration_change_percent" - $"avg_duration_percent_rate") / $"stddev_duration_percent_rate") > Z_SCORE_THRESHOLD && $"stddev_duration_percent_rate" =!= 0),
              true
            ).otherwise(false)
          ).drop(
            "prev_temp", "prev_duration", "prev_timestamp", "time_diff_minutes",
            "temp_change_rate_per_minute", "duration_change_percent",
            "avg_temp_rate", "stddev_temp_rate", "avg_duration_percent_rate", "stddev_duration_percent_rate"
          )

          // 4. Logical Anomaly (Stuck Status Anomaly)
          val logicalAnomalyTemp = rateChangeTemp.withColumn(
            "is_stuck_status_anomaly",
            when(
              ($"status" === "free" && $"duration" > MAX_DURATION_FOR_FREE_STATUS_STUCK) ||
              ($"status" === "occupied" && $"duration" > MIN_DURATION_FOR_OCCUPIED_STATUS_STUCK),
              true
            ).otherwise(false)
          )
          
          // 5. Contextual Pattern Anomaly (Simulated AI)
          val contextualPatternAnomalyTemp = logicalAnomalyTemp.withColumn(
            "is_contextual_pattern_anomaly",
            when(
              ($"status" === "occupied" && $"duration" < SHORT_OCCUPIED_DURATION_THRESHOLD && $"temperature" > HIGH_TEMPERATURE_THRESHOLD),
              true
            ).otherwise(false)
          ).select("sensor_id", "timestamp", "is_contextual_pattern_anomaly")


          // --- Combine all anomalies (excluding sensor_failure for now, it's handled separately) ---
          val anomaliesForCurrentBatchDF = logicalAnomalyTemp
            .join(contextualPatternAnomalyTemp, Seq("sensor_id", "timestamp"), "left_outer")
            .withColumn("is_contextual_pattern_anomaly", coalesce($"is_contextual_pattern_anomaly", lit(false)))


          // --- Calculate final_status for CURRENTLY REPORTING SENSORS ---
          val finalStatusForReportingSensorsDF = anomaliesForCurrentBatchDF.withColumn(
            "final_status",
            when(
              $"status" === "malfunction" ||
              $"is_physical_value_anomaly" ||
              $"is_z_score_anomaly" ||
              $"is_rate_of_change_anomaly" ||
              $"is_stuck_status_anomaly" ||
              $"is_contextual_pattern_anomaly",
              lit("malfunction")
            )
            .otherwise($"status") // Default to original status if no anomaly
          )
          .drop(
            "is_physical_value_anomaly",
            "is_z_score_anomaly",
            "is_rate_of_change_anomaly",
            "is_stuck_status_anomaly",
            "is_contextual_pattern_anomaly"
          )

          // --- Write original sensor data to 'sensor_data' table (always append) ---
          finalStatusForReportingSensorsDF.select(
            $"sensor_id",
            $"timestamp",
            $"duration",
            $"status", // Original status from sensor
            $"temperature",
            $"vehicle_license_plate"
          ).write
            .format("org.apache.spark.sql.cassandra")
            .options(Map("keyspace" -> "parking", "table" -> "sensor_data"))
            .mode("append")
            .save()

          // --- Phase 1 Write: Update current parking status for REPORTING SENSORS ---
          // This updates their 'last_updated' and calculated 'status'
          finalStatusForReportingSensorsDF.select(
            $"sensor_id",
            $"timestamp".as("last_updated"), // The timestamp of the current valid reading
            $"parking_lot_id".as("parking_lot_name"),
            $"slot_id",
            $"final_status".as("status") // Final calculated status for this reporting sensor
          ).write
            .format("org.apache.spark.sql.cassandra")
            .options(Map("keyspace" -> "parking", "table" -> "parking_spot_current_status"))
            .mode("append") // Cassandra will overwrite the row if Primary Key (sensor_id) exists
            .save()

          println(s"=== Batch $batchId: Data for REPORTING sensors processed and written to Cassandra. ===")
          println("Reporting sensor statuses in this batch:")
          finalStatusForReportingSensorsDF.select(
              $"sensor_id",
              $"timestamp",
              $"status".as("original_status"),
              $"final_status"
          ).show(false)

        } else {
            println(s"Batch $batchId: No valid and enriched sensor data from Kafka to process.")
        }
      } else {
        println(s"Batch $batchId: Received empty DataFrame from Kafka. Skipping reporting sensor processing.")
      }

      // --- Phase 2: Detect and update status for NON-REPORTING SENSORS (Sensor Failure) ---
      val allCurrentSensorsInDB = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "parking", "table" -> "parking_spot_current_status"))
          .load()
          .select("sensor_id", "last_updated", "parking_lot_name", "slot_id") // Include lot_name and slot_id for the update

      // Get sensor_ids that reported in this *current* batch
      val reportingSensorIdsInThisBatch = batchDF.select("sensor_id").distinct()

      // Find sensors in DB that *did not* report in this batch AND are overdue
      val nonReportingFailedSensors = allCurrentSensorsInDB
          .join(reportingSensorIdsInThisBatch, Seq("sensor_id"), "left_anti") // Exclude sensors that reported NOW
          .withColumn("time_diff_seconds", (unix_timestamp(lit(now)) - unix_timestamp($"last_updated")))
          .filter($"time_diff_seconds" > FAILURE_THRESHOLD_SECONDS)
          .withColumn("status", lit("sensor_failure")) // Explicitly set to sensor_failure
          // Keep original last_updated from Cassandra for auditing failed state
          .select("sensor_id", "last_updated", "parking_lot_name", "slot_id", "status")

      if (!nonReportingFailedSensors.isEmpty) {
          println(s"Batch $batchId: Detected and updating ${nonReportingFailedSensors.count()} NON-REPORTING sensor(s) to 'sensor_failure'.")
          nonReportingFailedSensors.show(false) // For debugging
          nonReportingFailedSensors.write
              .format("org.apache.spark.sql.cassandra")
              .options(Map("keyspace" -> "parking", "table" -> "parking_spot_current_status"))
              .mode("append") // Upsert: updates existing rows based on Primary Key (sensor_id)
              .save()
      } else {
          println(s"Batch $batchId: No non-reporting sensors detected for 'sensor_failure' update.")
      }

      println("--- End of batch processing for Batch " + batchId + " ---")

    }.start().awaitTermination()
  }
}