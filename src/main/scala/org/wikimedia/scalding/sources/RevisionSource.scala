package org.wikimedia.scalding.sources

import com.twitter.scalding.avro.PackedAvroSource
import com.twitter.scalding._
import org.wikimedia.mediawiki.RevisionSchema

// "/user/otto/dump_medium/avro/1415374651"
object RevisionSource {
  def apply(path: String) = TypedPipe.from(PackedAvroSource[RevisionSchema](path))
}