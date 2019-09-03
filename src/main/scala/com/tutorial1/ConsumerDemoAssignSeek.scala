package com.tutorial1

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

object ConsumerDemoAssignSeek extends App {

  val logger: Logger = LoggerFactory.getLogger("ConsumerDemo")

  val server = "localhost:9092"
  val topic = "first_topic"

  val props = new Properties()

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](props)

  def partitionToReadFrom = new TopicPartition(topic, 0)

  def offsetsToReadFrom: Long = 15

  consumer.assign(util.Arrays.asList(partitionToReadFrom))

  consumer.seek(partitionToReadFrom, offsetsToReadFrom)

  while (true) {
    def records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

    records.asScala.take(5).foreach { r =>
      logger.info("1 record")
      logger.info("Key: " + r.key + ", Value: " + r.value)
      logger.info("Partition: " + r.partition + ", Offset: " + r.offset)
    }
  }


  logger.info("Exiting the application")

}