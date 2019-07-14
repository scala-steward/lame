package lame

import akka.NotUsed
import akka.stream.{Attributes}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import lame.gzip.CompressionUtils.SimpleLinearGraphStage
import lame.gzip.DeflateCompressor
import java.util.zip.Deflater
import java.util.zip.CRC32

object BlockGzip {

  val MaxUncompressed = 65536 - 10 - 26 // 10 is GZIP overhead if data is uncompressable and gzip level is 0
  // 26 is gzip header + trailer
  val MaxCompressed = 65536 - 26 // 26 is gzip header + trailer

  case class State(
      data: ByteString,
      emit: ByteString,
      deflater: Deflater,
      crc32: CRC32
  ) {
    private val noCompressionDeflater = new Deflater(0, true)
    def update(bs: ByteString) = copy(data = data ++ bs)
    def needsInput = data.size < MaxUncompressed
    def clear = copy(emit = ByteString.empty)
    def compress =
      if (data.isEmpty)
        this
      else {
        def header(compressed: ByteString): ByteString =
          ByteString(
            0x1F, // ID1
            0x8B, // ID2
            8, // CM = Deflate
            4, // FLG
            0, // MTIME 1
            0, // MTIME 2
            0, // MTIME 3
            0, // MTIME 4
            0, // XFL
            0, // OS
            6, // XLEN 1
            0, // XLEN 2
            66, // SI1 B
            67, // SI2 C
            2, // subfield length 1
            0, // subfield length 2
            compressed.size + 25, // BSIZE 1
            (compressed.size + 25) >> 8 // BSIZE 2
          )

        val byteReader = new ByteReader(data)
        val input =
          byteReader.take(math.min(MaxUncompressed, byteReader.remainingSize))
        crc32.reset()
        crc32.update(input.toArray)
        val trailer: ByteString = {
          def int32(i: Int): ByteString =
            ByteString(i, i >> 8, i >> 16, i >> 24)
          val crc = crc32.getValue.toInt
          val trailer = int32(crc) ++ int32(input.size)

          trailer
        }

        val buffer =
          new Array[Byte](math.max(input.size, DeflateCompressor.MinBufferSize))

        val compressed = {
          deflater.reset()
          deflater.setInput(input.toArray)
          deflater.finish()
          val proposed =
            DeflateCompressor.drainDeflater(deflater, buffer, flush = true)
          if (proposed.size > MaxCompressed) {
            noCompressionDeflater.reset()
            noCompressionDeflater.setInput(input.toArray)
            noCompressionDeflater.finish()
            val bs = DeflateCompressor.drainDeflater(
              noCompressionDeflater,
              buffer,
              flush = true
            )
            bs
          } else proposed
        }

        val block = header(compressed) ++ compressed ++ trailer

        State(
          data = byteReader.remainingData,
          emit = emit ++ block,
          deflater = deflater,
          crc32 = crc32
        )

      }
  }

  def apply(
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  ): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph {
      new SimpleLinearGraphStage[ByteString] {
        override def createLogic(
            inheritedAttributes: Attributes
        ): GraphStageLogic =
          new GraphStageLogic(shape) with InHandler with OutHandler {

            var state = State(
              ByteString.empty,
              ByteString.empty,
              customDeflater.getOrElse(
                () => new Deflater(compressionLevel, true)
              )(),
              new CRC32
            )

            val bgzipTrailer = ByteString(
              0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06,
              0x00, 0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00,
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            )

            override def onPush(): Unit = {
              state = state.update(grab(in))
              if (!state.needsInput) {
                state = state.compress
              }
              if (state.emit.nonEmpty && isAvailable(out)) {
                push(out, state.emit)
                state = state.clear
              }

              if (state.needsInput) {
                pull(in)
              }
            }

            override def onPull(): Unit = {
              if (state.emit.nonEmpty) {
                push(out, state.emit)
                state = state.clear
              }
              if (!hasBeenPulled(in) && state.needsInput) {
                pull(in)
              }
            }

            override def onUpstreamFinish(): Unit = {
              while (state.data.nonEmpty) {
                state = state.compress
              }
              if (state.emit.nonEmpty) {
                emit(out, state.emit ++ bgzipTrailer)
                state = state.clear
              } else {
                emit(out, bgzipTrailer)
              }
              completeStage()
            }

            override def postStop(): Unit = state.deflater.end()

            setHandlers(in, out, this)
          }
      }
    }

}
