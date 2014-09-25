package org.wikimedia.scalding

import com.twitter.scalding._
import com.twitter.scalding.source._
import com.twitter.scalding.typed.TDsl
import org.apache.hadoop.io.{LongWritable, Text}

import TDsl._

// org.apache.hadoop.io.compress.SnappyCodec
class SimpleJob(args: Args) extends Job(args) {

  TypedPipe.from(WritableSequenceFile[LongWritable, Text](args("input")))
    .limit(10)
    .map { case (lw, sw) => lw.get -> sw.toString }
    .write(TypedTsv[(Long, String)](args("output")))
}
