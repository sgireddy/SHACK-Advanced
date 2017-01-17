package batch
import org.apache.spark.{SparkConf, SparkContext}
import config.{Contexts, Settings}
import models.{Activity, ProductActivityByReferrer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import scala.pickling.Defaults._
import scala.pickling.json._
import utils._
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/7/17 */
object BatchRDD extends App {
  import Settings._
  //val conf = new SparkConf().setAppName(appName).setMaster(s"${sparkMaster}[${numCores}]")
  //val sc = new SparkContext(conf)
  val sc = Contexts.getSparkContext(appName)
  val sqlContext = new SQLContext(sc)
  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val sourceFile = tmpFile
  val input: RDD[String] = sc.textFile(sourceFile)
  val inputRDD: RDD[Activity] = for {
    line <- input
    activity <- tryParse[Activity](line)
  } yield activity.copy(timeStamp = getTimeInMillisByHour(activity.timeStamp))


  val keyedByProduct: RDD[((Int, String, Long), Activity)] = inputRDD.keyBy( a => (a.productId, a.referrer, a.timeStamp)).cache()

  val visitsByCode = keyedByProduct.mapValues(v => v.actionCode match {
    case 0 => (1, 0, 0)
    case 1 => (1, 1, 0)
    case 2 => (1, 0, 1)
    case _ => (0, 0, 0)
  })

  val reduced = visitsByCode.reduceByKey((s, v) => ((s._1+ v._1, s._2 + v._2, s._3 + v._3))).sortByKey().collect()
  //Optionally Map to our model ProductActivityByReferrer
  val productActivityByReferer = reduced.map { r =>
    ProductActivityByReferrer(r._1._1, r._1._2, r._1._3, r._2._1.toLong, r._2._2.toLong, r._2._3.toLong)
  }
  productActivityByReferer
    .filter(p => p.productId == 1001 | p.productId == 1002)
    .foreach(println)

//Another approach, directly mapping to  ProductActivityByReferrer
//  val productActivity = inputRDD.map[ProductActivityByReferrer]{
//    v => v.actionCode match {
//      case 0 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp, 1, 0, 0)
//      case 1 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp, 1, 1, 0)
//      case 2 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp, 1, 0, 1)
//      case _ => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp, 0, 0, 0)
//    }
//  }
//  val keyed = productActivity.keyBy(p => (p.productId, p.referrer, p.timeStamp))
//  val d = keyed.reduceByKey((a, b) =>{
//    ProductActivityByReferrer(a.productId, a.referrer, a.timeStamp, a.viewCount+b.viewCount, a.cartCount+b.cartCount, a.purchaseCount+b.purchaseCount)
//  })


  println("Using DFF")
  //Using DataFrames
  val inputDF = inputRDD.toDF()
  inputDF.registerTempTable("activities")
  val activityByProductReferrer = sqlContext.sql(
    """
          select productId as product_id, referrer, timeStamp,
          sum(case when actionCode >= 0 then 1 else 0 end) as viewCount,
          sum(case when actionCode = 1 then 1 else 0 end) as cartCount,
          sum(case when actionCode = 2 then 1 else 0 end) as purchaseCount
          from activities
          where productId in (1001, 1002)
          group by productId, referrer, timeStamp
          order by product_id, referrer
          """
  )

  val result = activityByProductReferrer.map{
    r => ProductActivityByReferrer(r.getInt(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5))
  }

  result.foreach(println)

//  val result = activityByProductReferrer.map{
//    r => ((r.getInt(0), (r.getString(1)), (r.getLong(2))),
//      (ProductActivityByReferrer(r.getInt(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5))))
//  }


//  val aggregated = reduced.groupBy((0)((s: (Int, Int, Int), v: (Int, Int, Int)) => (s._1 + v._1, s._2 + v._2, s._3 + v._3))
//  //val applied = aggregated.apply(s: (Int, Int, Int), v: (Int, Int, Int)) => ((s._1+ v._1, s._2 + v._2, s._3 + v._3)))
  //val reduced = visitsByCode.reduceByKey((s: (Int, Int, Int), v: (Int, Int, Int)) => ((s._1+ v._1, s._2 + v._2, s._3 + v._3))).cache()
  //val aggregated = reduced.aggregateByKey((s: (Int, Int, Int), v: (Int, Int, Int)) => ((s._1+ v._1, s._2 + v._2, s._3 + v._3)))
//  reduced.takeOrdered(10).foreach(println(_))
//  val productActivityByReferrer = keyedByProduct.mapValues((v: Activity) => {
//    v.actionCode match {
//      case 0 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp/(1000 * 60 * 60), 1L, 0L, 0L)
//      case 1 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp/(1000 * 60 * 60), 0L, 1L, 0L)
//      case 2 => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp/(1000 * 60 * 60), 0L, 0L, 1L)
//      case _ => ProductActivityByReferrer(v.productId, v.referrer, v.timeStamp/(1000 * 60 * 60), 0L, 0L, 0L)
//    }
//  })
//  //val aggredaged = visitsByCode.aggregateByKey((s: (Int, Int, Int), v: (Int, Int, Int)) => ( s._1+ v._1, s._2 + v._2, s._3 + v._3))
//  //keyedByProduct.mapValues(a => a.referrer)
//  keyedByProduct.saveAsTextFile(s"${hdfsPath}/activities/${System.currentTimeMillis()}")
//  println(keyedByProduct.count())
//  val visitorsByProduct = keyedByProduct.mapValues( a => (a.userId)).distinct().countByKey()
//  keyedByProduct.toDF().write.partitionBy("._1").mode(SaveMode.Append).parquet(s"${hdfsPath}/activities")
//  val sample = visitorsByProduct.take(100)
//  println(sample)
  /****** Do more stuff ****/
}