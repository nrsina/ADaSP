package info.nourian.Actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.routing.{BroadcastRoutingLogic, Routee, Router}
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Entities._
import optimus.algebra.Expression
import optimus.optimization.MPModel

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._

object CentralMonitor {
  def props(controllers: immutable.IndexedSeq[Routee]): Props =
    Props(new CentralMonitor(controllers))
      .withDispatcher("pinned-dispatcher")
}

class CentralMonitor(controllers: immutable.IndexedSeq[Routee]) extends Actor
  with ActorLogging
  with Timers {
  import optimus.optimization.enums._
  import optimus.optimization.model._
  private val thresholdsMap : mutable.Map[ActorRef, Long] = mutable.Map.empty
  private var router = Router(BroadcastRoutingLogic())

  override def preStart(): Unit = {
    router = router withRoutees controllers
    timers.startSingleTimer(TickKey, CheckServiceTime, 10 second)
  }

  def receive: Receive = {
    case CheckServiceTime =>
      doRequestServiceTime()
    case ServiceTime(st) =>
      doGetServiceTime(st)
  }

  def doRequestServiceTime(): Unit = {
    log.info("Send CheckServiceTime from CentralMonitor")
    thresholdsMap.clear
    router.route(CheckServiceTime, self)
  }

  def doGetServiceTime(st: Long): Unit = {
    if (thresholdsMap.keySet contains sender)
      thresholdsMap(sender) = st
    else
      thresholdsMap += (sender -> st)
    val childNum = router.routees.size
    log.info("thresholdMap Size: " + thresholdsMap.size + " | childNum: " + childNum)
    if (thresholdsMap.size >= childNum) {
      runLPSolver()
      timers.startSingleTimer(TickKey, CheckServiceTime, 10 second)
    }
  }

  def runLPSolver(): Unit = {
    implicit val model = MPModel(SolverLib.oJSolver)
    val lpResultMap : mutable.Map[ActorRef, MPVar] = mutable.Map.empty
    var exp: Expression = null
    var const: Expression = null
    val refThres = ConfigurationFactory.getConfig().WaitTimeThreshold
    val min = refThres * 0.25
    val max = refThres * 0.75
    log info "Defining LP Params"
    var i = 1
    for ((actor, time) <- thresholdsMap) {
      val mPVar = MPFloatVar("x" + i, min, max)
      if (exp == null)
        exp = time * mPVar
      else
        exp += time * mPVar
      if (const == null)
        const = mPVar
      else
        const += mPVar
      lpResultMap += (actor -> mPVar)
      i+=1
    }
    model add (const := ConfigurationFactory.getConfig().WaitTimeThreshold)
    model maximize exp
    log info "Start Solving LP"
    model.start()
    for ((actor, result) <- lpResultMap) {
      val value = result.value.get
      sendOptimalThreshold(actor, value)
    }
    model.release
    log info "End Solving LP"
  }

  def sendOptimalThreshold(actor: ActorRef, value: Double): Unit = {
    log info value + " for " + actor.path.name
    actor ! OptimalThreshold(value)
  }

}
