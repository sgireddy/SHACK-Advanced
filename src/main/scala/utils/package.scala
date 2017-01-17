import java.util.Calendar

import kafka.common.TopicAndPartition
import models.Activity
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

package object utils {
  def errorHandler[T](tv: Try[T]) : Option[T] = {
    tv match {
      case Success(v) => Some(v)
      case Failure(ex) => {
        println(s"${ex.getMessage} ${System.lineSeparator()} ${ex.getStackTrace}")
        None
      }
    }
  }

  def tryParse[T](line: String ) (implicit m: TypeTag[T]) : Option[T] = {
    implicit val formats = DefaultFormats
    implicit val cl = ClassTag[T](m.mirror.runtimeClass(m.tpe))
    for {
      jv <- errorHandler(Try(parse(line)))
      v <- errorHandler(Try(jv.extract[T]))
    } yield v
  }

  def getTimeInMillisByHour(timeInMillis: Long) = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timeInMillis)
    val offset = ((cal.get(Calendar.MINUTE) * 60) + cal.get(Calendar.SECOND) ) * 1000
    (timeInMillis - offset) - ((timeInMillis - offset) % 9999)
  }

//  def rddToRDDActivity(input: RDD[(String, String)]) = {
//    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges
//
//    input.mapPartitionsWithIndex( (index, it) => {
//      val or = offsetRanges(index)
//      var inputProps: Map[String, String] = Map()
////        Map("topic"-> or.topic, "kafkaPartition" -> or.partition.toString,
////                  "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)
//      val activities = for {
//        (k, v) <- it
//        activity <- tryParse[Activity](v)
//        //activity.inputProps <- inputProps
//      } yield activity
//    })
//  }
//
//
//  def getOffsetRanges(input: RDD[(String, String)]) = {
//    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges
//    input.mapPartitionsWithIndex()
//
//
//    input.mapPartitionsWithIndex( {(index, it) =>
//      val or = offsetRanges(index)
//      val props = Map("topic"-> or.topic, "kafkaPartition" -> or.partition.toString,
//        "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)
//    })
//  }
}
