package com.intel.hibench.sparkbench.streaming

import com.intel.hibench.common.HiBenchConfig
import com.intel.hibench.common.streaming.{
  TestCase,
  StreamBenchConfig,
  Platform,
  ConfigLoader
}
import com.intel.hibench.common.streaming.metrics.MetricsUtil
import com.intel.hibench.sparkbench.streaming.util.SparkBenchConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerConfig
import com.intel.hibench.sparkbench.streaming.application._

object RunBench {

  def main(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    // Load configuration
    val master = conf.getProperty(HiBenchConfig.SPARK_MASTER)

    val batchInterval =
      conf.getProperty(StreamBenchConfig.SPARK_BATCH_INTERVAL).toInt
    val receiverNumber =
      conf.getProperty(StreamBenchConfig.SPARK_RECEIVER_NUMBER).toInt
    val copies = conf.getProperty(StreamBenchConfig.SPARK_STORAGE_LEVEL).toInt
    val enableWAL =
      conf.getProperty(StreamBenchConfig.SPARK_ENABLE_WAL).toBoolean
    val checkPointPath =
      conf.getProperty(StreamBenchConfig.SPARK_CHECKPOINT_PATH)
    val directMode =
      conf.getProperty(StreamBenchConfig.SPARK_USE_DIRECT_MODE).toBoolean
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val brokerList = conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST)
    val debugMode = conf.getProperty(StreamBenchConfig.DEBUG_MODE).toBoolean
    val recordPerInterval =
      conf.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL).toLong
    val intervalSpan: Int =
      conf.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN).toInt

    val windowDuration: Long =
      conf.getProperty(StreamBenchConfig.FixWINDOW_DURATION).toLong
    val windowSlideStep: Long =
      conf.getProperty(StreamBenchConfig.FixWINDOW_SLIDESTEP).toLong

    val coreNumber = conf
      .getProperty(HiBenchConfig.YARN_EXECUTOR_NUMBER)
      .toInt * conf.getProperty(HiBenchConfig.YARN_EXECUTOR_CORES).toInt

    val producerNum =
      conf.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER).toInt
    val reporterTopic = MetricsUtil.getTopic(
      Platform.SPARK,
      topic,
      producerNum,
      recordPerInterval,
      intervalSpan
    )
    println("Reporter Topic: " + reporterTopic)
    val reporterTopicPartitions =
      conf.getProperty(StreamBenchConfig.KAFKA_TOPIC_PARTITIONS).toInt
    MetricsUtil.createTopic(zkHost, reporterTopic, reporterTopicPartitions)

    val probability =
      conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble
    // init SparkBenchConfig, it will be passed into every test case
    val config = SparkBenchConfig(
      master,
      benchName,
      batchInterval,
      receiverNumber,
      copies,
      enableWAL,
      checkPointPath,
      directMode,
      zkHost,
      consumerGroup,
      topic,
      reporterTopic,
      brokerList,
      debugMode,
      coreNumber,
      probability,
      windowDuration,
      windowSlideStep
    )

    run(config)
  }

  private def run(config: SparkBenchConfig) {
    // select test case based on given benchName
    val testCase: BenchBase = TestCase.withValue(config.benchName) match {
      case TestCase.IDENTITY    => new Identity()
      case TestCase.REPARTITION => new Repartition()
      case TestCase.WORDCOUNT   => new WordCount()
      case TestCase.FIXWINDOW =>
        new FixWindow(config.windowDuration, config.windowSlideStep)
      case other =>
        throw new Exception(s"test case ${other} is not supported")
    }

    // defind streaming context
    val conf =
      new SparkConf().setMaster(config.master).setAppName(config.benchName)
    val ssc = new StreamingContext(conf, Milliseconds(config.batchInterval))
    ssc.checkpoint(config.checkpointPath)

    if (!config.debugMode) {
      ssc.sparkContext.setLogLevel("ERROR")
    }

    val lines =
      // Direct mode with low-level Kafka API
      KafkaUtils
        .createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](
            Set(config.sourceTopic),
            config.kafkaParams
          )
        )
    // .map(_.value)

    val parsedLines = lines.map(record => (record.key.toLong, record.value))
    testCase.process(parsedLines, config)

    ssc.start()
    ssc.awaitTermination()
  }
}
