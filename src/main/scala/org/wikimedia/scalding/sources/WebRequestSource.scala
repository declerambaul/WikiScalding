package org.wikimedia.scalding.sources

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.scalding._
import com.twitter.scalding.source._
import java.util.TimeZone
import org.apache.hadoop.io.{LongWritable, Text}
import org.wikimedia.scalding.WebRequestText

object WebRequestsSource {
  def apply(
      source: String)(
      implicit dateRange: DateRange,
      tz: TimeZone,
      dp: DateParser): TypedPipe[(LongWritable, Text)] = {
    val pattern = TimePathedSource.YEAR_MONTH_DAY_HOUR
    val step = TimePathedSource.stepSize(pattern, tz)
    val dateSuffixes = Globifier(pattern)(tz).globify(dateRange)
    val path = "/wmf/data/raw/webrequest/%s/hourly".format(source)
    println(dateSuffixes.map(path + _ + "/*"))
    val pipe = MultipleWritableSequenceFiles[LongWritable, Text](dateSuffixes.map(path + _ + "/*"))
    TypedPipe.from(pipe)
  }
}

object WebRequestTextSource {
  val mapper = new ObjectMapper()
  mapper.registerModule(new DefaultScalaModule)

  def apply()(
      implicit dateRange: DateRange,
      tz: TimeZone,
      dp: DateParser) = WebRequestsSource("webrequest_text")
    .map { case (lw, sw) => lw.get -> sw.toString }
    .map { case (_, requestString) =>
      mapper.readValue(requestString, classOf[WebRequestText])
    }
}

