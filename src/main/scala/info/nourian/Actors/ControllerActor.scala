package info.nourian.Actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Address, Deploy, Props, Timers}
import akka.remote.RemoteScope
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Routee, Router}
import com.github.tototoshi.csv.CSVWriter
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple
import info.nourian.Entities.Operator.Operator
import info.nourian.Entities._
import info.nourian.Queries.Query

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._

object ControllerActor extends ControllerProps {

  def props(prev: ActorRef, op: Operator, f: Any => Any, qosCheck: Boolean = true, initChilds: Int = 1): Props =
    Props(new ControllerActor(prev, op, f, qosCheck, initChilds))
      .withDispatcher("pinned-dispatcher")

  def remoteProps(prev: ActorRef, op: Operator, remoteAddress: Address, qosCheck: Boolean = true, initChilds: Int = 1): Props =
    Props(new ControllerActor(prev, op, null, qosCheck, initChilds))
      .withDispatcher("pinned-dispatcher")
      .withDeploy(Deploy(scope = RemoteScope(remoteAddress)))

  def childProp(op: Operator, f: Any => Any): Props = {
    op match {
      case Operator.Source =>
        SourceActor props f.asInstanceOf[Any => Tuple]
      case Operator.Transformation =>
        TransformationActor props f.asInstanceOf[Any => Tuple]
      case Operator.Sink =>
        SinkActor props f.asInstanceOf[Tuple => Any]
      case _ =>
        println("Case _ in childProp method")
        Props()
    }
  }
}

class ControllerActor(prev: ActorRef, op: Operator, var f: Any => Any, qosCheck: Boolean, initChilds: Int) extends Actor
  with ActorLogging
  with Timers {

  protected val wtimeMap : mutable.Map[ActorRef, (Int, Int)] = mutable.Map.empty
  protected val actorName: String = self.path.name
  protected var router = Router(BroadcastRoutingLogic())
  var routees : immutable.IndexedSeq[Routee] = immutable.IndexedSeq.empty
  var violationUpCount: Int = 0
  var violationDownCount: Int = 0
  var threshold: Double = 0
  var serviceReq: (Boolean, ActorRef) = (false, null)
  val timePoint: Long = System.currentTimeMillis()

  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter

  val dateTime: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm"))
  val writer: CSVWriter = CSVWriter.open(actorName + "_" + dateTime + ".csv", append = true)

  override def preStart(): Unit = {
    if (f == null)
      f = Query.getQuery
    threshold = ConfigurationFactory.getConfig().WaitTimeThreshold / (ConfigurationFactory.getConfig().Machines.size - 2) //Size - 2 = Only Transformation Operators.
    //if (qosCheck)
    writer.writeRow(List("Time", "WaitingTime", "Utilization", "Workers"))
    //timePoint = System.currentTimeMillis()
  }

  override def postStop(): Unit = {
    writer.close
  }

  def receive: Receive = {
    case StartJob =>
      doStartJob()
    case ScaleUp(n) =>
      doScaleUp(n) //scale up and add new operator address in broadcast router
    case ScaleDown(n) =>
      doScaleDown(n) //scale down and remove operator address from broadcast router
    case NewRoutee(x) =>
      doAddRoutee(x) //Add address of the new operator in next subquery
    case DeadRoutee(x) =>
      doRemoveRoutee(x) //remove address of the removed operator in next subquery
    case CheckQoS =>
      doCheckQoS()
    case QoSMetrics(w, u, st) =>
      doDecideScale(w, u, st)
    case CheckServiceTime =>
      doCheckServiceTime()
    case OptimalThreshold(t) =>
      doSetThreshold(t)
    case other =>
      doRouteToRoutees(other)
  }

  def doStartJob(): Unit = {
    self ! ScaleUp(initChilds)
    //if (qosCheck)
    timers.startSingleTimer(TickKey, CheckQoS, ConfigurationFactory.getConfig().SecondsBetweenQoSCheck second)
    //sender() ! Done
    log.info(actorName + " Started")
  }

  def doScaleUp(n: Int): Unit = {
    var newOps: Set[ActorRef] = Set.empty
    for (s <- 1 to n) {
      val child = context actorOf ControllerActor.childProp(op, f)
      newOps = newOps + child
      if (routees.nonEmpty)
        child ! RouteesSeq(routees)
      router = router addRoutee child
    }
    log.info(actorName + " Scaled Up!")
    log.info(actorName + " - Routees Size: " + router.routees.size)
    if (prev != null)
      prev ! NewRoutee(newOps)
  }

  def doScaleDown(n: Int): Unit = {
    var deadOps: Set[ActorRef] = Set.empty
    for (s <- 1 to n) {
      val smallest = wtimeMap.minBy(_._2._1)._1
      deadOps = deadOps + smallest
      wtimeMap -= smallest
      router = router removeRoutee smallest
    }
    prev ! DeadRoutee(deadOps)
    //deadOps foreach (_ ! PoisonPill)
    log.info(actorName + " Scaled Down!")
    log.info(actorName + " - Routees Size: " + router.routees.size)
  }

  def doAddRoutee(refs: Set[ActorRef]): Unit = {
    refs foreach (r => routees = routees :+ ActorRefRoutee(r))
    router.route(NewRoutee(refs), self)
  }

  def doRemoveRoutee(refs: Set[ActorRef]): Unit = {
    refs foreach (r => routees = routees filterNot (_ == ActorRefRoutee(r)))
    router.route(DeadRoutee(refs), self)
  }

  def doCheckQoS(): Unit = {
    wtimeMap.clear
    router.route(SendQoSData, self)
    log.info(actorName + " - CheckQoS msg sent to children")
  }

  def doDecideScale(waitingTime: Int, util: Int, serviceTime: Long): Unit = {
    if (wtimeMap.keySet.contains(sender()))
      wtimeMap(sender()) = (waitingTime, util)
    else {
      wtimeMap += (sender() -> (waitingTime, util))
      val opWriter: CSVWriter = CSVWriter.open(sender().path.name + "_" + dateTime + ".csv", append = true)
      val timePassed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - timePoint)
      opWriter.writeRow(List(timePassed, waitingTime))
      opWriter.close
    }
    //log.info(actorName + " - Map size: " + wtimeMap.size)
    val childNum = router.routees.size
    if (wtimeMap.size >= childNum) {
      val waitAvg = wtimeMap.foldLeft(0)(_+_._2._1) / wtimeMap.size
      val utilAvg = wtimeMap.foldLeft(0)(_+_._2._2) / wtimeMap.size
      //log.info(actorName + " - Map Count: " + wtimeMap.size)
      log.info(actorName + " - Children Size: " + childNum)
      log.info(actorName + " - Average waiting time is " + waitAvg)
      log.info(actorName + " - Average utilization is " + utilAvg)
      if (waitingTime != 0 || util != 0 || serviceTime != 0) {
        val timePassed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - timePoint)
        writer.writeRow(List(timePassed, waitAvg, utilAvg, childNum))
      }
      if (!qosCheck) {
        wtimeMap.clear
        timers.startSingleTimer(TickKey, CheckQoS, ConfigurationFactory.getConfig().SecondsBetweenQoSCheck second)
        return
      }
      var optimalOps = optimalOperatorsNum(waitAvg.asInstanceOf[Double], childNum.asInstanceOf[Double])
      if (optimalOps > 18) //Runtime.getRuntime.availableProcessors
        optimalOps = 18
      val scale = optimalOps - childNum
      if (scale > 0) {
        violationUpCount += 1
        log.info(actorName + " Avg waiting time > (" + threshold + "). Violation = " + violationUpCount)
        violationDownCount = 0
        if (violationUpCount >= ConfigurationFactory.getConfig().ViolationUpLimit) {
          log.info(actorName + " Scaling Up")
          self ! ScaleUp(scale)
          violationUpCount = 0
        }
      } else if (scale < 0 && utilAvg < ConfigurationFactory.getConfig().MinimumUtilizationForScaleDown && childNum > 1) {
        violationDownCount += 1
        log.info(actorName + " Avg waiting time < (" + threshold + "). Violation = " + violationDownCount)
        violationUpCount = 0
        if (violationDownCount >= ConfigurationFactory.getConfig().ViolationDownLimit) {
          log.info(actorName + "Scaling Down")
          if (ConfigurationFactory.getConfig().EnableFastScaleDown && utilAvg <= ConfigurationFactory.getConfig().MinimumUtilForFastScaleDown) {
            var count = Math.abs(scale)
            if (count > ConfigurationFactory.getConfig().MaxScaleDownNodes)
              count = ConfigurationFactory.getConfig().MaxScaleDownNodes
            if (count >= childNum)
              count = childNum - 1
            self ! ScaleDown(count)
          } else
            self ! ScaleDown(1)
          violationDownCount = 0
        }
      } else {
        violationUpCount = 0
        violationDownCount = 0
      }
      timers.startSingleTimer(TickKey, CheckQoS, ConfigurationFactory.getConfig().SecondsBetweenQoSCheck second)
    }
    answerServiceTimeReq(serviceTime)
  }

  def doCheckServiceTime(): Unit = {
    serviceReq = (true, sender)
  }

  def doSetThreshold(t: Double): Unit = {
    threshold = t
  }

  def doRouteToRoutees(other: Any): Unit = {
    router.route(other, self)
  }

  def optimalOperatorsNum(avg: Double, op: Double): Int = {
    Math.ceil(avg * op / threshold).asInstanceOf[Int]
  }

  def answerServiceTimeReq(serviceTime: Long): Unit = {
    if (serviceReq._1) {
      serviceReq._2 ! ServiceTime(serviceTime)
      log.info("Sent ServiceTime from " + actorName + " to Controller")
      serviceReq = (false, null)
    }
  }
}
