package consumer

import org.apache.spark.streaming.Seconds
/** Created by Shashi Gireddy on 1/4/17 */
object SparkConsumer extends App {

  val spark = config.Contexts.getSparkContext(config.Settings.appName)
  val batchDuration = Seconds(4)
  val ssc = config.Contexts
    .getStreamingContext(jobs.ProductActivityByReferrer.ProductActivityByReferrerJob.promoEfficiencyJob, spark, batchDuration)
  ssc.start()
  ssc.awaitTermination()

}
