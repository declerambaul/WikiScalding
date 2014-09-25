package org.wikimedia.scalding

import com.twitter.scalding._
import java.util.TimeZone
import org.wikimedia.scalding.sources.WebRequestTextSource

object WebRequestTextFilter {
  val excludedIps = Set(
    "10.128.0.",
    "208.80.152.",
    "208.80.153.",
    "208.80.154.",
    "208.80.155.",
    "91.198.174.")

  // returns true if we want to count that request
  def apply(req: WebRequestText): Boolean = {
    (excludedIps.filter(ip => req.ip.startsWith(ip)).isEmpty || !req.x_forwarded_for.equals("-"))
    !req.uri_path.startsWith("/wiki/Special:CentralAutoLogin/")
  }
}

// based on https://gerrit.wikimedia.org/r/#/c/160636/2/webstatscollector.hql
object WebRequestTextExtractors {

  // why? not using for now
  val shortNames = Map(
    "wikipedia" -> "",
    "wikibooks" -> ".b",
    "wiktionary" -> ".d",
    "wikimediafoundation" -> ".f",
    "wikimedia" -> ".m",
    "wikinews" -> ".n",
    "wikiquote" -> ".q",
    "wikisource" -> ".s",
    "wikiversity" -> ".v",
    "wikivoyage" -> ".voy",
    "mediawiki" -> ".w",
    "wikidata" -> ".wd")

  lazy val languageAndProject: WebRequestText => (String, String) = { req: WebRequestText =>
    req.uri_host.split('.').toList match {
      case lang :: project :: "org" :: Nil => (lang, project)
      case _ => ("unknown", "unknown")
    }
  }

  // SPLIT(TRANSLATE(SUBSTR(uri_path, 7), ' ', '_'), '#')[0] article,
  lazy val article: WebRequestText => Option[String] = { req: WebRequestText => Option(req.uri_path)
    .filter(_.nonEmpty)
    .filter(_.startsWith("/wiki"))
    .map(_.split("#").head)
  }
}

case class PageView(
  article: String,
  language: String,
  project: String,
  request: WebRequestText)

object PageView {
  import WebRequestTextExtractors._
  def fromWebRequestText(req: WebRequestText): PageView = {
    val (language, project) = languageAndProject(req)
    PageView(article(req).getOrElse("unknown"), language, project, req)
  }
}

trait PageViews {

  implicit val tz: TimeZone
  implicit val dp: DateParser
  implicit val dateRange: DateRange

  lazy val pageViews: TypedPipe[PageView] = WebRequestTextSource()
    .filter(WebRequestTextFilter(_))
    .map { req: WebRequestText => PageView.fromWebRequestText(req) }
    .fork

  lazy val (validPageviews, utfWeirdPageViews) = {
    val validEither = pageViews.map { case pw =>
      if (pw.article.contains('%')) Right(pw) else Left(pw)
    }
    (validEither.collect { case Left(pw) => pw },
      validEither.collect { case Right(pw) => pw} )
  }

  // for debugging only. These should be either parsed correctly or discarded with good reason.
  // hour, project, language, count of weird articles
  lazy val utfWeirdCounts = utfWeirdPageViews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.project, pw.language) -> 1L
  }.sumByLocalKeys.sumByKey.withReducers(10)

  // these are explicit examples. for production this should be an keyexpansion to all possible value permutations (Option[X1], Option[X2], ...)

  // hour, project, count
  lazy val byProject = validPageviews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.project) -> 1L
  }.sumByLocalKeys.sumByKey.withReducers(10)

  // hour, project, count
  lazy val byLanguage = validPageviews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.language) -> 1L
  }.sumByLocalKeys.sumByKey.withReducers(10)

  // hour, article, project, language, count
  lazy val byArticle = validPageviews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.article) -> 1L
    }.sumByLocalKeys.sumByKey.withReducers(64)

  // hour, article, project, language, count
  lazy val maxKey = validPageviews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.article, pw.project, pw.language) -> 1L
    }.sumByLocalKeys.sumByKey.withReducers(128)
}

object PageViews extends PageViews {
  implicit val dp = DateParser.default
  implicit val tz = DateOps.UTC
  implicit val dateRange = DateRange(RichDate.now - Days(2)(tz), RichDate.now - Days(1)(tz))
}

class PageViewsJob(args: Args) extends Job(args)  { // DefaultDateRangeJob
  import PageViews._

  val outDir = args("out_directory")

  byProject
    .map { case ((h, p), c) => (h, p, c) }
    .write(TypedTsv(outDir + "/by_project"))

  byArticle
    .map { case ((h, a), c) => (h, a, c) }
    .write(TypedTsv(outDir + "/by_article"))

  byLanguage
    .map { case ((h, l), c) => (h, l, c) }
    .write(TypedTsv(outDir + "/by_language"))

  utfWeirdCounts
    .map { case ((h, p, l), c) => (h, p, l, c) }
    .write(TypedTsv(outDir + "/by_weird_utf8"))

  maxKey
    .map { case ((h, a, p, l), c) => (h, a, p, l, c) }
    .write(TypedTsv(outDir + "/by_all"))
}
