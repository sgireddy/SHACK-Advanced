package generators

import config.Settings
import models.Activity
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord, RecordMetadata}
import scala.concurrent._
import scala.util.Random
import Settings._
/** Created by Shashi Gireddy on 1/3/17 */
object KafkaStreamGenerator extends App {

  implicit val formats = DefaultFormats
  val rnd = new Random()
  val products = getProducts()

  for(bno <- 0 to numBatches) {
    val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](kafkaProps)
    println(kafkaProducer.partitionsFor(kafkaTopic))
    for (cnt <- 1 to batchSize) {
        val productId = productRangeStart + rnd.nextInt((productRangeEnd - productRangeStart) + 1)
        val customerId = userRangeStart + rnd.nextInt((userRangeEnd - userRangeStart) + 1)
        val margin = getProductMargin(productId)
        val activity = if (cnt % promoAvailabilityFactor > 0) {
          val pd = getProductDiscount(productId)
          val cd = getCartDiscount(productId)
          Activity(System.currentTimeMillis(), productId, customerId, getRandomReferrer(), products(productId), pd, cd, getAction(pd, cd), margin)
        } else {
          Activity(System.currentTimeMillis(), productId, customerId, "site", products(productId), 0, 0, getAction(), margin)
        }
        val producerRecord = new ProducerRecord(kafkaTopic, write(activity))
        val p = Promise[(RecordMetadata, Exception)]()
        kafkaProducer.send(producerRecord)
  //      val response = kafkaProducer.send(producerRecord)
  //        , new Callback {
  //        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
  //          p.success((metadata, exception))
  //        }
  //      })
  //      val reqMetadata = response.get(2, TimeUnit.SECONDS)
  //      val (callbackMeta, callbackEx) = Await.result(p.future, new FiniteDuration(3, TimeUnit.SECONDS))
        Thread.sleep(messageDelay)
    }
    println(s"Closing session... Sleeping... will wake up in  ${streamDelay} ms Batch Number: ${bno}")
    kafkaProducer.close()
    Thread.sleep(streamDelay)
  }
  println("Job Complete...")
}