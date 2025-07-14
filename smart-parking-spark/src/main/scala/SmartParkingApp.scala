import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SmartParkingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SmartParkingKafkaToCassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "smart-parking-cassandra-1")
      .getOrCreate()

    import spark.implicits._

    val schema = new StructType()
      .add("sensor_id", StringType)
      .add("parking_lot_name", StringType)
      .add("slot_id", IntegerType)
      .add("reading_ts", StringType)
      .add("status", StringType)
      .add("duration", IntegerType)
      .add("temperature", DoubleType)
      .add("vehicle_license_plate", StringType)

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "parking-sensor-data")
      .option("startingOffsets", "latest")
      .load()

    val parsedDf = kafkaDf.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"reading_ts"))

    val sensorDataDf = parsedDf.select(
      $"sensor_id", $"timestamp", $"duration", $"status",
      $"temperature", $"vehicle_license_plate"
    )

    val currentStatusDf = parsedDf.select(
      $"sensor_id", $"parking_lot_name", $"slot_id", $"timestamp".alias("last_updated"),
      $"status", $"temperature", $"vehicle_license_plate"
    )

    sensorDataDf.writeStream
      .foreachBatch((batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) => {
        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "parking")
          .option("table", "sensor_data")
          .mode("append")
          .save()
      })
      .outputMode("update")
      .start()

    currentStatusDf.writeStream
      .foreachBatch((batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) => {
        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "parking")
          .option("table", "parking_spot_current_status")
          .mode("append")
          .save()
      })
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
