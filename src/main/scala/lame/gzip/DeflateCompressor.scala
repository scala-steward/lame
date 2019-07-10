/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package lame.gzip

import java.util.zip.Deflater

import akka.util.{ByteString, ByteStringBuilder}

import scala.annotation.tailrec

object DeflateCompressor {
  val MinBufferSize = 1024

  @tailrec
  def drainDeflater(
      deflater: Deflater,
      buffer: Array[Byte],
      result: ByteStringBuilder = new ByteStringBuilder()
  ): ByteString = {
    val len = deflater.deflate(buffer)
    if (len > 0) {
      result ++= ByteString.fromArray(buffer, 0, len)
      drainDeflater(deflater, buffer, result)
    } else {
      require(deflater.needsInput())
      result.result()
    }
  }
}
