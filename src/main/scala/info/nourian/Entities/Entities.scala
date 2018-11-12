package info.nourian.Entities

import akka.actor.ActorRef
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import akka.routing._
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple

import scala.collection.immutable

trait Control

case object StartJob extends Control
case class NewRoutee(x: Set[ActorRef]) extends Control
case class DeadRoutee(x: Set[ActorRef]) extends Control
case class ScaleUp(x: Int) extends Control
case class ScaleDown(x: Int) extends Control
case object SendQoSData extends Control
case class QueueVariables(meanBetweenArrivals: Long, coefficientOfVariationArrival: Double) extends Control
case class QoSMetrics(waitingTime: Int, util: Int, serviceTime: Long) extends Control
case class RouteesSeq(rs: immutable.IndexedSeq[Routee]) extends Control
case class ServiceTime(st: Long)
case class OptimalThreshold(thres: Double)
case object CheckServiceTime
case object StartStream extends Control
case object Done extends Control
case class WindowProps(winSize: Int, paneSize: Int, enablePane: Boolean = ConfigurationFactory.getConfig().usePaneProcessorInKeyedOperator)
case object CalculateStats extends Control


//Keyed Transformation control messages
//case class SiblingsAndRoutees(sibs: immutable.IndexedSeq[Routee], rs: immutable.IndexedSeq[Routee]) extends Control
case class NotifyScaled(rs: immutable.IndexedSeq[Routee]) extends Control
case object GracefulShutdown extends Control //Controller sends this message to operators that need to shutdown

case class Pane(t: Tuple) extends Control with ConsistentHashable {
  override def consistentHashKey: Any = t.consistentHashKey
}
case class KeyState(key: Any, list: List[Tuple]) extends Control with ConsistentHashable {
  override def consistentHashKey: Any = key
}
case class PaneState(key: Any, list: List[Tuple]) extends Control with ConsistentHashable {
  override def consistentHashKey: Any = key
}

case object Operator extends Enumeration {
  type Operator = Value
  val Source, Transformation, StatefulTransformation, Sink = Value
}

case object DeployApproach extends Enumeration {
  type Deploy = Value
  val Local, LocalOnPort, Remote = Value
}

case object TickKey
case object CheckQoS