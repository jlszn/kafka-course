package com.tutorial1

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}

object ProducerDemoWithCallback extends App {

  private val logger: Logger = LoggerFactory.getLogger("ProducerDemoWithCallback")

  private val server = "localhost:9092"

  private val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  private def sendRecord(n: Int): Future[RecordMetadata] = {

    def record: ProducerRecord[String, String] =
      new ProducerRecord[String, String]("first_topic", "hello world " + n)

    producer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          logger.info("\n" + "Received metadata. \n" +
            "Topic equals: " + recordMetadata.topic() + "\n" +
            "Partition is: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp() + "\n")
        } else {
          logger.error("Error while producing", exception)
        }
      }
    })

  }

  for (n <- 0 to 10) sendRecord(n)

  producer.close()

}
