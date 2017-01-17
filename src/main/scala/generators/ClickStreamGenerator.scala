package generators

import java.io.FileWriter
import config.Settings
import models._
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import scala.util.Random
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object ClickStreamGenerator extends App {

  import Settings._
  implicit val formats = DefaultFormats
  val rnd = new Random()
  val products = getProducts()
  val fw = new FileWriter(tmpFile, true)

  for(cnt <- 1 to batchSize){
    val productId = productRangeStart + rnd.nextInt((productRangeEnd - productRangeStart) + 1)
    val customerId = userRangeStart + rnd.nextInt((userRangeEnd - userRangeStart) + 1)
    val margin = getProductMargin(productId)
    val activity = if(cnt % promoAvailabilityFactor > 0){
      val pd = getProductDiscount(productId)
      val cd = getCartDiscount(productId)
      Activity(System.currentTimeMillis(), productId, customerId, getRandomReferrer(), products(productId), pd, cd, getAction(pd, cd), margin)
    } else {
      Activity(System.currentTimeMillis(), productId, customerId, "site", products(productId), 0, 0, getAction(), margin)
    }
    val jsonString = write(activity) + System.lineSeparator()
    fw.append(jsonString)
  }

  fw.close()
}