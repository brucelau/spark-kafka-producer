name := "Spark Fu Kafka Producer"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0",
    "org.apache.kafka" %% "kafka" % "0.8.1.1",
    "joda-time" % "joda-time" % "2.7"
  )
  
