package org.wikimedia.scalding

import com.twitter.scalding._
import java.util.TimeZone
import org.wikimedia.scalding.sources.WebRequestTextSource

// todo unit tests. is this actually doint something useful?
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
// todo unit tests
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
    // todo - getOrElse("unknown") is used to test the coverage of the article extractor.
    // we should dig into the different reasons for why it can fail
    PageView(article(req).getOrElse("unknown"), language, project, req)
  }
}

trait PageViews {

  implicit val tz: TimeZone
  implicit val dp: DateParser
  implicit val dateRange: DateRange

  // pipe of pageview events
  lazy val pageViews: TypedPipe[PageView] = WebRequestTextSource()
    .filter(WebRequestTextFilter(_))
    .map { req: WebRequestText => PageView.fromWebRequestText(req) }
    .fork

  /*
  Below are explicit examples. For production this should be an key expansion to
  all possible/desired permutations (Option[X1], Option[X2], ...), which will result
  in a single job. Currently there is a sumByKey (i.e. a map/reduce step) for each codec
  which we don't actually need to do.

  The map side aggregation (sumByLocalKeys) should be a win here because of the smaller cardinality of
  our key space and nearly time ordered nature of the requests logs (e.g. many events processed by the same
  mapper will fall in the same hour bucket).
   */

  // for debugging only. These should be either parsed correctly or discarded with good reason.
  lazy val (validPageviews, utfWeirdPageViews) = {
    val validEither = pageViews.map { case pw =>
      // What are these url encoded uri_path? uri_path":"/wiki/%E3%82%B1%E3%82%A4%E3%83%88%E3%83%BB%E3%82%AD%E3%83%A3%E3%83%97%E3%82%B7%E3%83%A7%E3%83%BC"
      // There are many valid articles that hava a % in the name, but there are a large number of whese weird ones
      if (pw.article.contains('%')) Right(pw) else Left(pw)
    }
    (validEither.collect { case Left(pw) => pw },
      validEither.collect { case Right(pw) => pw} )
  }

  // debugging output. These should be either parsed correctly or discarded with good reason.
  // hour, project, language, count of weird articles
  lazy val utfWeirdCounts = utfWeirdPageViews.map { case pw =>
    //for now we throw if malformed date, we want to know if this happens
    val hour = dp.parse(pw.request.dt).get.timestamp / (1000*60*60)
    (hour, pw.project, pw.language) -> 1L
  }.sumByLocalKeys.sumByKey.withReducers(10)

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

// singleton to be used in the scalding repl when working interactively
object PageViews extends PageViews {
  implicit val dp = DateParser.default
  implicit val tz = DateOps.UTC
  implicit val dateRange = DateRange(RichDate.now - Days(2)(tz), RichDate.now - Days(1)(tz))
}

// job class for deploying jobs to the cluster via command line
class PageViewsJob(args: Args) extends Job(args) with UtcDateRangeJob with PageViews {
  implicit val dp = DateParser.default

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
