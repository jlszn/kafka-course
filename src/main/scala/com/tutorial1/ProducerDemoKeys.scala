package com.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}

object ProducerDemoKeys extends App {

  val logger: Logger = LoggerFactory.getLogger("ProducerDemoKeys")

  val server = "localhost:9092"
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  private def sendRecord(n: Int): RecordMetadata = {

    val topic = "first_topic"
    val value = "hello world " + n
    val key = "id_" + n

    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, key, value)

    logger.info("\n\nKEY: " + key + "\n")

    producer.send(record, new Callback() {
      override def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit =
        if (exception == null) {
          logger.info("\n\n" + "RECEIVED METADATA \n" +
            "Topic equals: " + recordMetadata.topic() + "\n" +
            "Partition is: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp() + "\n")
        } else {
          logger.error("Error while producing", exception)
        }
    }).get()

  }

  for (n <- 0 to 10) sendRecord(n)

  producer.close()

}