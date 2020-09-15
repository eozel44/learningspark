package workshop10.config

import com.typesafe.config.ConfigFactory

object settings {

  private val config = ConfigFactory.load()

  object sparkStreaming {
    private val spark = config.getConfig("streamer.spark")

    lazy val master = spark.getString("master")
    lazy val name = spark.getString("app_name")
  }

  object KafkaConsumer {
    private val producer = config.getConfig("streamer.kafka")

    lazy val interval = producer.getInt("batch_interval")
    lazy val kafkaTopic = producer.getString("topic")
    lazy val offset = producer.getString("offset")

    lazy val bootstrapServers = producer.getString("bootstrap_servers")
    lazy val keySerializer = producer.getString("key_serializer")
    lazy val valueSerializer = producer.getString("value_serializer")
    lazy val groupName = producer.getString("group_name")
  }

}
