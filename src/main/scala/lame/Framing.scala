package lame

import akka.stream.scaladsl._
import akka.util.ByteString

object Framing {
  def delimiter(
    delimiter: Byte,
    maximumFrameLength: Int): Flow[ByteString, ByteString, akka.NotUsed] =
  Flow[ByteString].statefulMapConcat { () =>
    var buffer = ByteString.empty

    def indexOfDelimiterIn(bs: ByteString): Int = {
      var i = 0
      var idx = -1
      while (idx == -1 && i < bs.length) {
        if (bs.apply(i) == delimiter) {
          idx = i
        }
        i += 1
      }
      idx
    }

    val empty = ByteString("")

    { elem =>
      var idx = indexOfDelimiterIn(elem)
      if (idx == -1) {
        if (buffer.size + elem.size > maximumFrameLength) {
          throw new RuntimeException("Buffer too small")
        }
        buffer = buffer ++ elem
        Nil
      } else {
        val output = scala.collection.mutable.ArrayBuffer[ByteString]()
        var suffix = elem
        while (idx != -1) {

          val prefix = suffix.take(idx)
          val nextOutput = buffer ++ prefix
          buffer = empty
          output.append(nextOutput)

          suffix = suffix.drop(idx + 1)
          idx = indexOfDelimiterIn(suffix)
        }

        if (suffix.size > maximumFrameLength) {
          throw new RuntimeException("Buffer too small")
        }
        buffer = suffix
        output.toList
      }
    }

  }
}