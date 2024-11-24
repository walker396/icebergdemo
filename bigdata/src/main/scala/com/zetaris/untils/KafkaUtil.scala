package com.zetaris.utils

import com.zetaris.config.Config
import com.zetaris.untils.PropertiesUtil
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.asJavaCollectionConverter

/**
 * Kafka utility class for production and consumption
 **/
object KafkaUtil {
  /**
   * Kafka producer object
   */
  private val producer: KafkaProducer[String, String] = createProducer()

  private val adminClient: AdminClient = createAdmin()
  /**
   * Consumer configuration
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // Kafka cluster address
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil(Config.KAFKA_BOOTSTRAP_SERVER),
    // Key-value deserializer
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // Offset auto-commit
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ->  "true",
    // Offset reset to latest
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )
  /**
   * Consuming based on Spark Streaming, obtaining Kafka DStream
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }
  /**
   * Consuming based on Spark Streaming, with specified offsets
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offset: Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offset))
    kafkaDStream
  }
  def createAdmin(): AdminClient = {
    val adminProperties = new Properties()
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil(Config.KAFKA_BOOTSTRAP_SERVER))
    AdminClient.create(adminProperties)
  }

  /**
   * Create Kafka producer object
   */
  def createProducer(): KafkaProducer[String, String] = {
    // Producer configuration class
    val producerConfig = new Properties()
    // Kafka cluster address
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil(Config.KAFKA_BOOTSTRAP_SERVER))
    // Key-value serializer
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // Acknowledgment configuration
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
    // Idempotence configuration
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    // Create Kafka producer
    val _producer = new KafkaProducer[String, String](producerConfig)
    _producer
  }
  /**
   * Produce data (using default sticky partitioning)
   */
  def send(topic:String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }
  /**
   * Produce data (using specified key for partitioning)
   */
  def send(topic: String, msg: String, key:String) = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  def deleteTopic(topic: String) = {
    adminClient.deleteTopics(List(topic).asJavaCollection)
  }

  def createTopic(topic: String) = {
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    adminClient.createTopics(List(newTopic).asJavaCollection)
  }
  /**
   * Close producer object
   */
  def close = if (producer != null) {producer.close()}
  /**
   * Flush data from buffer to broker
   */
  def flush = producer.flush()
}
