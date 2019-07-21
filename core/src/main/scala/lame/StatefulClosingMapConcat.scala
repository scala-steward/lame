package lame

import akka.stream.{Attributes, Inlet, Outlet, FlowShape}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, GraphStage}
import scala.collection.immutable
import akka.stream.scaladsl.Flow

object StatefulClosingMapConcat {

  class StatefulClosingMapConcat[In, Out](
      val f: () => (
          In => immutable.Iterable[Out],
          () => immutable.Iterable[Out]
      )
  ) extends GraphStage[FlowShape[In, Out]] {
    val in = Inlet[In]("StatefulClosingMapConcat.in")
    val out = Outlet[Out]("StatefulClosingMapConcat.out")
    override val shape = FlowShape(in, out)

    def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        val (pushIntoState, closeState) = f()

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          emitMultiple(out, pushIntoState(grab(in)))
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          emitMultiple(out, closeState())
          completeStage()
        }

        override def onPull(): Unit =
          if (!isClosed(in) && !hasBeenPulled(in)) {
            pull(in)
          }

      }

    override def toString = "StatefulClosingMapConcat"

  }

  def apply[In, Out](
      f: () => (In => immutable.Iterable[Out], () => immutable.Iterable[Out])
  ) = Flow[In].via(new StatefulClosingMapConcat(f))
}
