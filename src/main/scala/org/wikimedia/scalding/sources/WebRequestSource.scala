package org.wikimedia.scalding.sources

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.scalding._
import com.twitter.scalding.source._
import java.util.TimeZone
import org.apache.hadoop.io.{LongWritable, Text}
import org.wikimedia.scalding.WebRequestText

// a source that provides a pipe of the k/v tupls stored in the raw sequence files
object WikipediaSequenceFilesSource {
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

// Typed source for text web requests. It parses the json into the domain class WebRequestText
object WebRequestTextSource {
  val mapper = new ObjectMapper()
  mapper.registerModule(new DefaultScalaModule)

  def apply()(
      implicit dateRange: DateRange,
      tz: TimeZone,
      dp: DateParser) = WikipediaSequenceFilesSource("webrequest_text")
    .map { case (lw, sw) => lw.get -> sw.toString }
    .map { case (_, requestString) =>
      mapper.readValue(requestString, classOf[WebRequestText])
    }
}

