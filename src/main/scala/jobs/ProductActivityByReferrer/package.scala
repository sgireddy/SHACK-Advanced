package jobs
import config.Settings.{kafkaDirectParams, kafkaTopic}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import models.ProductActivityByReferrer
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, State, StateSpec, StreamingContext}

package object ProductActivityByReferrer {
  def mapProductActivityByReferrer = (k: (Int, String, Long), v: Option[ProductActivityByReferrer], state: State[(Long, Long, Long)]) => {

    var (viewCount, cartCount, purchaseCount) = state.getOption().getOrElse((0L, 0L, 0L))
    val newValue = v match {
      case Some(p: ProductActivityByReferrer) => (p.viewCount, p.cartCount, p.purchaseCount)
      case _ => (0L, 0L, 0L)
    }
    viewCount += newValue._1
    cartCount += newValue._2
    purchaseCount += newValue._3
    state.update(viewCount, cartCount, purchaseCount)
  }

  val stateSpec = StateSpec.function(mapProductActivityByReferrer).timeout(Minutes(90))

  def getOffsets(sc: SparkContext) = {
    val cassandraContext = new CassandraSQLContext(sc)
    var fromOffsets: Map[TopicAndPartition, Long] = Map.empty
    val offsetDF = cassandraContext.sql(
      """
        |select topic, partition, max(until_offset) as until_offset
        |from promo.offsets
        |group by topic, partition
      """.stripMargin)
    fromOffsets = offsetDF.rdd.collect().map( o => {
      (TopicAndPartition(o.getAs[String]("topic"), o.getAs[Int]("partition")), o.getAs[Long]("until_offset") + 1)
    }
    ).toMap
    fromOffsets
  }

  def getKafkaDirectStream(ssc: StreamingContext) = {
    val fromOffsets = getOffsets(ssc.sparkContext)
    fromOffsets.isEmpty match {
      case true =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaDirectParams, Set(kafkaTopic)
        )
      case false =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, kafkaDirectParams, fromOffsets, { mmd: MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
        )
    }
  }
}