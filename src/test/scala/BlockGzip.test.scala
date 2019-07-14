package lame
import org.scalatest._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._

class BlockGzipSuite extends FunSuite with Matchers {
  def randomData(size: Int) = {
    val random = new scala.util.Random
    val buf = Array.fill[Byte](size)(0)
    random.nextBytes(buf)
    ByteString(buf)
  }

  test("correctness - 256") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw = randomData(256)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }
  test("correctness - 0") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw = randomData(0)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }
  test("correctness - 90KB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw = randomData(1024*90)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }
  test("correctness - 270KB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw = randomData(1024*90)
    val data2 = Await
      .result(
        Source
          .apply(List(raw,raw,raw))
          .via(lame.BlockGzip())
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    (raw++raw++raw) shouldBe data2
    AS.terminate()
  }
  test("correctness - 120KB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw = randomData(1024*40)
    val data2 = Await
      .result(
        Source
          .apply(List(raw,raw,raw))
          .via(lame.BlockGzip())
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    (raw++raw++raw) shouldBe data2
    AS.terminate()
  }
  

}
