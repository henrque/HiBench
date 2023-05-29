/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hibench.common.streaming.metrics

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

class KafkaConsumer2(zookeeperConnect: String, topic: String, partition: Int) {
  private val CLIENT_ID = "metrics_reader"
  private val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , zookeeperConnect)
  props.put("group.id", CLIENT_ID)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
  private val config = new ConsumerConfig(props)
  private val consumer = createConsumer
  private val topicPartition = new TopicPartition(topic, partition)
  consumer.assign(java.util.Collections.singletonList(topicPartition))
  private val earliestOffset = consumer
    .beginningOffsets(java.util.Collections.singletonList(topicPartition))
    .get(topicPartition)
  private var nextOffset: Long = earliestOffset
  private var iterator
      : java.util.Iterator[ConsumerRecord[String, Array[Byte]]] = getIterator(
    nextOffset
  )
  def next(): Array[Byte] = {
    val record = iterator.next()
    nextOffset = record.offset() + 1
    record.value()
  }

  def hasNext: Boolean = {
    @annotation.tailrec
    def hasNextHelper(
        iter: java.util.Iterator[ConsumerRecord[String, Array[Byte]]],
        newIterator: Boolean
    ): Boolean = {
      if (iter.hasNext) true
      else if (newIterator) false
      else {
        iterator = getIterator(nextOffset)
        hasNextHelper(iterator, newIterator = true)
      }
    }
    hasNextHelper(iterator, newIterator = false)
  }

  def close(): Unit = {
    consumer.close()
  }

private def createConsumer: KafkaConsumer[String, Array[Byte]] = {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, zookeeperConnect)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  new KafkaConsumer[String, Array[Byte]](props)
}

  private def getIterator(
      offset: Long
  ): java.util.Iterator[ConsumerRecord[String, Array[Byte]]] = {
    consumer.seek(topicPartition, offset)
    val records = consumer.poll(0)
    records.iterator()
  }
}
