package info.nourian.Actors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{ActorRef, Address, Deploy, Props}
import akka.remote.RemoteScope
import com.github.tototoshi.csv.CSVWriter
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple
import info.nourian.Entities.Operator.Operator
import info.nourian.Entities._
import info.nourian.Queries.Query

object StatefulController extends ControllerProps {

  def props(prev: ActorRef, op: Operator, f: Any => Any, winProps: WindowProps, qosCheck: Boolean = true, initChilds: Int = 1): Props =
    Props(new StatefulController(prev, op, f, winProps, qosCheck, initChilds))
      .withDispatcher("pinned-dispatcher")

  def remoteProps(prev: ActorRef, op: Operator, remoteAddress: Address, winProps: WindowProps, qosCheck: Boolean = true, initChilds: Int = 1): Props =
    Props(new StatefulController(prev, op, null, winProps, qosCheck, initChilds))
      .withDispatcher("pinned-dispatcher")
      .withDeploy(Deploy(scope = RemoteScope(remoteAddress)))

  def childProp(op: Operator, f: Any => Any, winProps: WindowProps): Props = {
    op match {
      case Operator.StatefulTransformation =>
        KeyedTransformation.props[Int](f.asInstanceOf[Any => Tuple], winProps)
      case _ =>
        println("Case _ in childProp method")
        Props()
    }
  }
}

class StatefulController(prev: ActorRef, op: Operator, var func: Any => Any, winProps: WindowProps, qosCheck: Boolean, initChilds: Int)
  extends ControllerActor(prev, op, null, qosCheck, initChilds) {

  override val actorName: String = self.path.name
  override val dateTime: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm"))
  override val writer: CSVWriter = CSVWriter.open(actorName + "_" + dateTime + ".csv", append = true)
  override val timePoint: Long = System.currentTimeMillis()

  override def preStart(): Unit = {
    if (func == null)
      func = Query.getQuery
    threshold = ConfigurationFactory.getConfig().WaitTimeThreshold / (ConfigurationFactory.getConfig().Machines.size - 2) //Size - 2 = Only Transformation Operators.
    writer.writeRow(List("Time", "WaitingTime", "Utilization", "Workers"))
    //timePoint = System.currentTimeMillis()
  }

  override def receive: Receive = {
    case StartJob =>
      doStartJob()
    case ScaleUp(n) =>
      doScaleUp(n)
      notifyScalingChanges() //Notify child actors of the same ActorSystem
    case ScaleDown(n) =>
      doScaleDown(n)
    case NewRoutee(x) =>
      doAddRoutee(x)
    case DeadRoutee(x) =>
      doRemoveRoutee(x)
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

  def notifyScalingChanges(): Unit = {
    router route (NotifyScaled(router.routees), self)
    log info actorName + "NotifyScaling Changes sent"
  }

  override def doScaleUp(n: Int): Unit = {
    var newOps: Set[ActorRef] = Set.empty
    for (s <- 1 to n) {
      val child = context actorOf StatefulController.childProp(op, func, winProps)
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

  override def doScaleDown(n: Int): Unit = {
    var deadOps: Set[ActorRef] = Set.empty
    for (s <- 1 to n) {
      val smallest = wtimeMap.minBy(_._2._1)._1
      deadOps = deadOps + smallest
      wtimeMap -= smallest
      router = router removeRoutee smallest
    }
    prev ! DeadRoutee(deadOps)
    notifyScalingChanges()
    deadOps foreach (_ ! GracefulShutdown) //Instead of sending PoisonPill, Send a custom shutdown message to do a graceful shutdown by moving actor states to another actor
    log.info(actorName + " Scaled Down!")
    log.info(actorName + " - Routees Size: " + router.routees.size)
  }

}
