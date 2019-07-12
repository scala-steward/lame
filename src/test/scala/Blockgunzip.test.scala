package lame
import org.scalatest._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._

class BlockGunzipSuite extends FunSuite with Matchers {
  def randomData(size:Int) ={
    val random = new scala.util.Random
    val buf = Array.fill[Byte](size)(0)
            random.nextBytes(buf)
    val os = new java.io.ByteArrayOutputStream(8192)
    val bos = new htsjdk.samtools.util.BlockCompressedOutputStream(os,null.asInstanceOf[java.io.File])
    bos.write(buf)
    bos.close
    (ByteString(buf),ByteString(os.toByteArray()))
  }
   

  test("correctness - empty") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    println("start")
    val (raw,compressed) = randomData(0)
    val data2 = Await
      .result(
        Source.single(compressed)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    raw shouldBe data2
  }
  test("correctness - short") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    println("start")
    val (raw,compressed) = randomData(256)
    val data2 = Await
      .result(
        Source.single(compressed)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    raw shouldBe data2
  }
  test("correctness - 90kbyte") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    println("start")
    val (raw,compressed) = randomData(1024*90)
    val data2 = Await
      .result(
        Source.single(compressed)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    raw shouldBe data2
  }
  test("correctness - 1MB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    println("start")
    val (raw,compressed) = randomData(1024*1024)
    val data2 = Await
      .result(
        Source.single(compressed)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    raw shouldBe data2
  }
  test("correctness - 1MB - emit from 5th") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    println("start")
    val (raw,compressed) = randomData(1024*1024)
    val data2 = Await
      .result(
        Source.single(compressed)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 5))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    raw.drop(5) shouldBe data2
  }
}
