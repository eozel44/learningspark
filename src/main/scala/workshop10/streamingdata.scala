package workshop10

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import workshop10.config.settings

case class Location(
                     latitude: Double,
                     longitude: Double
                   )

case class SensorDataRaw(
                          data: Data
                        )

case class SensorDataFlat (

                            deviceId: String,
                            temperature: Long,
                            latitude: Double,
                            longitude: Double,
                            time: Long
                          )


case class Data (
                  deviceId: String,
                  temperature: Long,
                  location: Location,
                  time: Long
                )

object streamingdata {


    val kafkaProps = settings.KafkaConsumer
    val sparkProps = settings.sparkStreaming


    def startStreamingKafka(spark:SparkSession,sc:SparkContext): Unit = {
      import spark.implicits._

      //get kafka configs
      val topics = kafkaProps.kafkaTopic.split(",").toSeq
      val bootstrapServers = kafkaProps.bootstrapServers
      val offset = kafkaProps.offset
      val batchInterval = Seconds(kafkaProps.interval)
      val groupIds = kafkaProps.groupName


      println("==========================================================")
      println("||       Streaming Sensor Data From Kafka                ||")
      println("==========================================================")


      //Define streaming context with sparkContext and batchInterval
      val streamingContext = new StreamingContext(sc, batchInterval)


      //Parameters for kafka
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrapServers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> groupIds,
        "auto.offset.reset" -> offset,
        "enable.auto.commit" -> (true: java.lang.Boolean)
      )


      // crate dstreams
      val stream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      //    val sensorDataSchema = Encoders.product[SensorDataRaw].schema
      //    val sensorDataflatSchema = Encoders.product[SensorDataFlat].schema

      //perform some operation in each batch of streamed data
      stream.foreachRDD(rawRDD => {
        // Apply transformation for each rdd  obtained in batch Interval

        val rdd = rawRDD.map(_.value())

        //if dataframe is not empty convert to dataframe
        if (!rdd.isEmpty()) {

          val df = spark.read.json(rdd.toDS()).select("data.*")
            .select(
              "deviceId",
              "location.latitude",
              "location.longitude",
              "temperature",
              "time"
            )
            .withColumn("time", from_unixtime($"time" / 1000, "yyyy-MM-dd HH:mm:ssXXX"))

          df.printSchema

          df.show(false)
          writeToDB(df)
        }
      })


      streamingContext.start()

      try {
        streamingContext.awaitTermination()
        println("***************** streaming terminated  ***********************************")
      } catch {
        case e: Exception => {
          println("************* streaming exception caught in monitor thread  *****************")
        }
      }

      streamingContext.stop()


    }

    def writeToDB(df: DataFrame) = {

      println("writing db")

  }

    def main(args: Array[String]): Unit = {

      //create an entry point for spark streaming

      val spark = SparkSession
        .builder()
        .appName(sparkProps.name)
        .master(sparkProps.master)
        .getOrCreate()

      // funtion to start streaming
      startStreamingKafka(spark,spark.sparkContext)

    }

}
