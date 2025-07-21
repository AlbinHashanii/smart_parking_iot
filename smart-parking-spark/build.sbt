name := "SmartParkingApp"

version := "1.0"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0" // This line is crucial for MLlib
)