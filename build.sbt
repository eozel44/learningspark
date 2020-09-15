name := "learningspark"

//version of package
version := "0.1"

//version of Scala
scalaVersion := "2.12.10"

// spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql"  % "3.0.0",
  "org.apache.spark" %% "spark-avro" % "3.0.0",
  "org.apache.kafka" %% "kafka" % "2.6.0",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.0.0-preview",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.0.0"

)
