package batch

import config.Contexts
import config.Settings._
import models.{Activity, ProductActivityByReferrer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import utils.{getTimeInMillisByHour, tryParse}

object BatchDS {
  val sc = Contexts.getSparkContext(appName)
  val sqlContext = Contexts.getSqlContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val inputRDD = sc.textFile(tmpFile) //.as[Activity]
  val inputDS = {
    for {
      line <- inputRDD
      activity <- tryParse[Activity](line)
    } yield activity.copy(timeStamp = getTimeInMillisByHour(activity.timeStamp))
  }.toDS()

  /*TBD*/
}
