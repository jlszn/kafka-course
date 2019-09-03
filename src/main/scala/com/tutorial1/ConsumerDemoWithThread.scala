package com.tutorial1

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.Try

object ConsumerDemoWithThread extends App {

  val logger: Logger = LoggerFactory.getLogger("ConsumerDemo")

  val server: String = "localhost:9092"
  val groupId: String = "my-sixth-application"
  val topic: String = "first_topic"
  val latch = new CountDownLatch(1)

  def execute(): Unit = {
    def myConsumerRunnable: ConsumerRunnable = ConsumerRunnable(server, groupId, topic, latch)

    def thread = new Thread(myConsumerRunnable)

    logger.info("Creating the consumer thread")

    thread.start()

    Try(latch.await()).getOrElse(logger.error("Application got interrupted"))

    logger.info("Application is closing")

    Runtime.getRuntime.addShutdownHook(new Thread( () => {
      logger.info("Caught shutdown hook")
      myConsumerRunnable.shutdown()
      latch.await()
    }
  ))
  }

  execute()

  case class ConsumerRunnable(server: String,
                              groupId: String,
                              topic: String,
                              private val latch: CountDownLatch) extends Runnable {

    private val logger: Logger = LoggerFactory.getLogger("ConsumerThread")

    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    private val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic))

    override def run(): Unit = {
      Try {
        while (true) {
          val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

          records.asScala.foreach(r => {
            logger.info("Key: " + r.key + ", Value: " + r.value)
            logger.info("Partition: " + r.partition + ", Offset: " + r.offset)
          })

        }
      }.getOrElse(
        logger.info("Received shutdown info")
      )

      consumer.close()
      latch.countDown()
    }

    def shutdown(): Unit = consumer.wakeup()

    run()

  }


}
