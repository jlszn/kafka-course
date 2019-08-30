package com.tutorial1

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

object ConsumerDemoGroups {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger("ConsumerDemo")

    val server = "localhost:9092"
    val groupId = "my-eighth-application"
    val topic = "first_topic"

    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

      records.asScala.foreach(r => {
        logger.info("Key: " + r.key + ", Value: " + r.value)
        logger.info("Partition: " + r.partition + ", Offset: " + r.offset)
      })

    }
  }

}