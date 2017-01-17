package config

import java.lang.management.ManagementFactory
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object Settings {
  private val config = ConfigFactory.load()
  private val settings = config.getConfig("appsettings")
  lazy val kafkaTopic = settings.getString("kafka-topic")
  lazy val hdfsPath = settings.getString("hdfs_path")
  lazy val appName = settings.getString("app-name")
  lazy val sparkMaster = settings.getString("spark-master")
  lazy val numCores = settings.getString("spark-number-of-cores")
  lazy val batchSize = settings.getInt("batch-size")
  lazy val numBatches = settings.getInt("number-of-batches")
  lazy val productRangeStart = settings.getInt("product-range-start")
  lazy val productRangeEnd = settings.getInt("product-range-end")
  lazy val userRangeStart = settings.getInt("user-range-start")
  lazy val userRangeEnd = settings.getInt("user-range-end")
  lazy val promoAvailabilityFactor = settings.getInt("promo-availability-factor")
  lazy val messageDelay = settings.getInt("message-depaly-ms")
  lazy val streamDelay = settings.getInt("stream-delay-ms")
  lazy val tmpFile = settings.getString("tmp-file")

  lazy val kafkaProps: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "UserActivityProducer")
    //props.put("advertised.host.name", "localhost")
    props
  }

  val kafkaDirectParams = Map(
    "metadata.broker.list" -> "localhost:9092",
    "group.id" -> "lambda",
    "auto.offset.reset" -> "largest"
  )
}
