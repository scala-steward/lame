package lame

import akka.stream.scaladsl.Flow
import scala.concurrent.duration._

object Batch {
  def strictBatchWeighted[T](
      maxWeight: Long,
      cost: T => Long,
      maxDuration: FiniteDuration = 1 seconds
  )(reduce: (T, T) => T): Flow[T, T, akka.NotUsed] =
    Flow[T]
      .groupedWeightedWithin(maxWeight, maxDuration)(cost)
      .map(_.reduce(reduce))
}
