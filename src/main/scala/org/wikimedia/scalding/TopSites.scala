package org.wikimedia.scalding

import com.twitter.algebird._
import com.twitter.scalding._
import com.twitter.scalding.source._
import org.apache.hadoop.io.{LongWritable, Text}

// ignore this
// this was the first test job for algebird monoids
// it should mix in the PageViews aggregations and output topK on those
object TopSites {
  implicit val topKMonoid = new TopKMonoid[(String, Long)](100)(Ordering.by(_._2))

  val devices = Set("Windows", "iPhone", "iPad", "Android", "Macintosh" )
  val input = "/wmf/data/external/webrequest/webrequest_text/hourly/2014/07/16/12/"
  val Site = """sitename=(.*?)&""".r
  val UserAgent = """"user_agent":(.*?)",""".r

  val requests = TypedPipe.from(WritableSequenceFile[LongWritable, Text](input))
    .map { case (lw, sw) => lw.get -> sw.toString }

  lazy val parsed = requests.map { case (_, req) =>
    val site = Site.findFirstMatchIn(req).map(_.toString).getOrElse("no_sitename")
    val ua = UserAgent.findFirstMatchIn(req).map(_.toString).getOrElse("no_ua")
    (site,ua)
  }

  def topSites = parsed.join(parsed.map { case (s,_) => s -> 1L}.sumByKey.withReducers(24))
    .flatMap { case (site, (ua, sum)) =>
      devices.toList.filter { device => ua.contains(device) }
        .map { device => device -> topKMonoid.build((site,sum)) }
    }.sumByKey.withReducers(4)
     .mapValues { case topK => topK.items }
     .toTypedPipe
}