package workshop10


import org.apache.spark.sql.{Encoders, SparkSession}
import workshop10.config.settings

object streamingdatastructured {


  val kafkaProps = settings.KafkaConsumer
  val sparkProps = settings.sparkStreaming


  def startStreamingKafka(spark:SparkSession): Unit = {
    import spark.implicits._

    val schema = Encoders.product[SensorDataRaw].schema

    val topics = kafkaProps.kafkaTopic
    val bootstrapServers = kafkaProps.bootstrapServers
    val offset = kafkaProps.offset


    println("==========================================================")
    println("||               Kafka Streaming Kafka                  ||")
    println("==========================================================")


    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()


    df.selectExpr("CAST(value AS STRING)").show(false)

    val rdd = df.selectExpr("CAST(value AS STRING)").rdd


    val dfNew = spark.read.json(rdd.map(_.getString(0)).toDS())

    schema.printTreeString()

    dfNew.show(false)
    dfNew.printSchema()
  }

  def main(args: Array[String]): Unit = {

    //create an entry point for spark streaming

    val spark = SparkSession
      .builder()
      .appName(sparkProps.name)
      .master(sparkProps.master)
      .getOrCreate()

    // funtion to start streaming
    startStreamingKafka(spark)

  }

}
