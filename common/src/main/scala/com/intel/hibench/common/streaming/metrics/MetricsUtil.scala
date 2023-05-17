package com.intel.hibench.common.streaming.metrics

import com.intel.hibench.common.streaming.Platform
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConverters._

object MetricsUtil {
  val TOPIC_CONF_FILE_NAME = "metrics_topic.conf"

  def getTopic(platform: Platform, sourceTopic: String, producerNum: Int,
               recordPerInterval: Long, intervalSpan: Int): String = {
    val topic = s"${platform}_${sourceTopic}_${producerNum}_${recordPerInterval}" +
      s"_${intervalSpan}_${System.currentTimeMillis()}"
    println(s"metrics is being written to kafka topic $topic")
    topic
  }

  def createTopic(zkConnect: String, topic: String, partitions: Int): Unit = {
    val config = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> zkConnect,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
    ).asJava

    val adminClient = AdminClient.create(config)
    val newTopic = new NewTopic(topic, partitions, 1.toShort)
    adminClient.createTopics(List(newTopic).asJava).all().get()

    while (!adminClient.listTopics().names().get().contains(topic)) {
      Thread.sleep(100)
    }

    adminClient.close()
  }
}