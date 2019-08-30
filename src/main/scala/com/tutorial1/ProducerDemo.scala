package com.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object ProducerDemo extends App {

  private val server = "localhost:9092"

  private val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  val producerRecord: ProducerRecord[String, String] =
    new ProducerRecord[String, String]("first_topic", "hello world")

  producer.send(producerRecord)

  producer.close()

}
