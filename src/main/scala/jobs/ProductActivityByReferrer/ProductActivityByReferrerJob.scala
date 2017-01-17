package jobs.ProductActivityByReferrer

import config.Settings._
import models.{Activity, ProductActivityByReferrer}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/** Created by Shashi Gireddy on 1/4/17 */
object ProductActivityByReferrerJob {

  def promoEfficiencyJob(sc: SparkContext, duration: Duration): StreamingContext = {
    val ssc = new StreamingContext(sc, duration)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataPath = hdfsPath + "/activities/data"
    val kafkaDirectStream = getKafkaDirectStream(ssc)

    val activityDStream = kafkaDirectStream.transform(rdd => {
      for {
        (k, v) <- rdd
        activity <- utils.tryParse[Activity](v)
      } yield activity.copy(timeStamp = activity.timeStamp/(1000 * 60 * 60))
    })

    //Save to HDFS
    activityDStream.foreachRDD( rdd => {
      val df = rdd.toDF()
        .selectExpr("timeStamp", "productId", "userId", "referrer", "retailPrice", "productDiscountPct", "cartDiscountPct", "actionCode", "marginPct")
      df.write.partitionBy("timeStamp", "productId").mode(SaveMode.Append).parquet(dataPath)
    })


    //Save Offsets to Cassandra
    kafkaDirectStream.foreachRDD( rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      sc.makeRDD[OffsetRange](offsetRanges).saveToCassandra("promo", "offsets") //, SomeColumns("topic", "partition", "fromOffset", "untilOffset")
    })


    val hourlyProductActivityByReferrer = activityDStream.transform( rdd => {
      val keyedByProduct = rdd.keyBy( a => (a.productId, a.referrer, a.timeStamp)).cache()
      val visitsByCode = keyedByProduct.mapValues(v => v.actionCode match {
        case 0 => (1, 0, 0)
        case 1 => (1, 1, 0)
        case 2 => (1, 0, 1)
        case _ => (0, 0, 0)
      })
      visitsByCode.map { r =>
        ((r._1._1, r._1._2, r._1._3),
        ProductActivityByReferrer(r._1._1, r._1._2, r._1._3, r._2._1.toLong, r._2._2.toLong, r._2._3.toLong))
      }
//Data Frames Approach
//      val df = rdd.toDF() //.selectExpr("timeStamp", "productId", "userId", "referrer", "retailPrice", "productDiscountPct", "cartDiscountPct", "actionCode", "marginPct")
//      df.registerTempTable("activities")
//      val activityByProductReferrer = sqlContext.sql(
//        """
//          select productId, referrer, timeStamp,
//          sum(case when actionCode = 0 then 1 else 1 end) as viewCount,
//          sum(case when actionCode = 1 then 1 else 0 end) as cartCount,
//          sum(case when actionCode = 2 then 1 else 0 end) as purchaseCount
//          from activities
//          group by productId, referrer, timeStamp
//          """
//      )
////    activityByProductReferrer.printSchema()
//      activityByProductReferrer.map{
//        r => ((r.getInt(0), (r.getString(1)), (r.getLong(2))),
//          (ProductActivityByReferrer(r.getInt(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5))))
//      }
    }).mapWithState(stateSpec)

    val snapshot = hourlyProductActivityByReferrer.stateSnapshots()
    snapshot.reduceByKeyAndWindow(
      (a, b) => b,
      (x, y) => x,
      Seconds(12)
    ).map(r => ProductActivityByReferrer(r._1._1, r._1._2, r._1._3, r._2._1, r._2._2, r._2._3))
     .saveToCassandra("promo", "product_activity_by_referrer_by_hr")
    //snapshot.print()
    ssc
  }
}