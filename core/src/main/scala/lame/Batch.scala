package lame

import akka.stream.scaladsl.Flow
import scala.concurrent.duration._
import akka.util.ByteString

object Batch {

  val reduceByteStrings = (s: Seq[ByteString]) => {
    val builder = ByteString.newBuilder
    s.foreach(b => builder.append(b))
    builder.result()
  }
  def strictBatchWeightedR[T](
      maxWeight: Long,
      cost: T => Long,
      maxDuration: FiniteDuration = 1 seconds
  )(reduce: (T, T) => T): Flow[T, T, akka.NotUsed] =
    Flow[T]
      .groupedWeightedWithin(maxWeight, maxDuration)(cost)
      .map(_.reduce(reduce))

  def strictBatchWeighted[T, K](
      maxWeight: Long,
      cost: T => Long,
      maxDuration: FiniteDuration = 1 seconds
  )(reduce: Seq[T] => K): Flow[T, K, akka.NotUsed] =
    Flow[T]
      .groupedWeightedWithin(maxWeight, maxDuration)(cost)
      .map(reduce)

  def strictBatchWeightedByteStrings(
      maxWeight: Long,
      cost: ByteString => Long,
      maxDuration: FiniteDuration = 1 seconds
  ): Flow[ByteString, ByteString, akka.NotUsed] =
    strictBatchWeighted[ByteString, ByteString](maxWeight, cost, maxDuration)(
      reduceByteStrings
    )
}
