package workshop9

import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Properties, UUID}

import config.settings
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

case class Location(
                     latitude: Double,
                     longitude: Double
                   )

case class IotData(
                    deviceId: UUID,
                    temperature: Int,
                    location: Location,
                    time: Long
                  ) {
  def toJSON(): String = {
    "{\"data\": {" +
      "\"deviceId\": \"" + deviceId + "\"," +
      "\"temperature\": " + temperature + "," +
      "\"location\": {" +
      "\"latitude\": " + location.latitude + "," +
      "\"longitude\": " + location.longitude + "}," +
      "\"time\": " + time +
      "}}"
  }
}

object kafkaproducer {


  val producerConfig = settings.Producer

  println("Application started!")

  println(producerConfig.bootstrapServers)
  println(producerConfig.keySerializer)
  println(producerConfig.valueSerializer)
  println(producerConfig.kafkaTopic)

  val LATITUDE_CONST: Float = 27.616677f
  val LONGITITUDE_CONST: Float = 85.387109f
  val uuid = UUID.randomUUID()


  val MIN_TEMP = 0
  val MAX_TEMP = 130
  val random = new scala.util.Random()
  //  }

  val kafkaProp = new Properties()
  kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfig.bootstrapServers)
  kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerConfig.keySerializer)
  kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerConfig.valueSerializer)
  kafkaProp.put(ProducerConfig.CLIENT_ID_CONFIG, producerConfig.groupName)
  kafkaProp.put("retries", "3")
  kafkaProp.put("linger.ms", "5")
  kafkaProp.put(ProducerConfig.ACKS_CONFIG, "all")


  val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](kafkaProp)



  def startSending() = {
    val runnable = new Runnable() {
      def run() = {

        val iotData = new IotData(
          uuid,
          MIN_TEMP + random.nextInt((MAX_TEMP - MIN_TEMP) + 1),
          Location(LATITUDE_CONST, LONGITITUDE_CONST),
          System.currentTimeMillis()
        )
        val iotJSONData = iotData.toJSON()

        val producerRecord = new ProducerRecord(producerConfig.kafkaTopic,"", iotJSONData)
        kafkaProducer.send(producerRecord)

        println(iotJSONData)
      }
    }
    val service = Executors.newSingleThreadScheduledExecutor
    service.scheduleAtFixedRate(runnable, 0, producerConfig.interval, TimeUnit.SECONDS)
  }

  def main(args: Array[String]): Unit = {

    println("Started sending Data")
    startSending()
    println("Application stopped!")

  }



}
