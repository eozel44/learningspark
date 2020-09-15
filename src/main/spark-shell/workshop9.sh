~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-test --from-beginning


//delete topic
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic sensor-test