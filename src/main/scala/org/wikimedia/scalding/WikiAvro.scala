package org.wikimedia.scalding

import org.wikimedia.mediawiki.RevisionSchema
import org.wikimedia.scalding.sources.RevisionSource
import com.twitter.scalding._
import com.twitter.algebird._
import scala.collection.JavaConverters._

import difflib.DiffUtils

object Playground {

  val badWords = Set("fuck", "shit", "cunt", "asshole", "douchebag", "douchebag")

  val x = RevisionSource("/user/otto/dump_medium/avro/1415374651/part-00000.avro")
    .filter(_.getPageId==1526)
    .groupBy(_.getPageId) // Grouped[PageId, Revision]
    .sortBy(_.getTimestamp.toString)
    .scanLeft( (new RevisionSchema, new RevisionSchema) ) { case ((previousRevision, diffedRevision), currentRevision) =>
      def lines(revision: RevisionSchema) = Option(revision.getText)
        .map(_.toString.split("\n").toList)
        .getOrElse(List.empty)
      val prevLines = lines(previousRevision)
      val currLines = lines(currentRevision)
      val textDiff = DiffUtils.diff(prevLines.asJava, currLines.asJava)
      val patchedString = textDiff.getDeltas().asScala.map { delta =>
        val original = delta.getOriginal.toString
        val revised = delta.getRevised.toString
        s"Original:\n$original\n\nRevised:\n$revised"
      }.mkString("\n\n\nDelta:\n")
      val copyRevisionSchema = RevisionSchema.newBuilder(currentRevision).build
      copyRevisionSchema.setText(patchedString)
      (currentRevision, copyRevisionSchema)
    } // Grouped[PageId, (Revision, Revision)]
    .map { case (_, (_, diffedRevision)) => diffedRevision }
}

