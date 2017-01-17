package config

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import config.Settings._
//import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
/** Created by Shashi Gireddy on 1/4/17 */
object Contexts {

  lazy val isIDE = ManagementFactory.getRuntimeMXBean(). getInputArguments().toString().indexOf("jdwp") >= 0

  def getSparkContext(appName: String) = {

    var checkpointDirectory = ""
    val conf = new SparkConf().setAppName(appName).set("spark.cassandra.connection.host", "127.0.0.1")

    if(isIDE){
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///home/sri/tmp"
    } else {
      checkpointDirectory = "hdfs://localhost:9000/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSqlContext(sc: SparkContext): SQLContext ={
    implicit val sqlContext = new SQLContext(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext,
                          sc: SparkContext,
                          batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => sc.setCheckpointDir(cp))
    ssc
  }
}