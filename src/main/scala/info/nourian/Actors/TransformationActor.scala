package info.nourian.Actors

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.dispatch.RequiresMessageQueue
import akka.routing.{Routee, Router}
import com.google.common.collect.EvictingQueue
import info.nourian.Configuration.{ConfigurationFactory, OpRoutingLogic}
import info.nourian.Data.Tuple
import info.nourian.Entities._
import info.nourian.Queue.OperatorQueueSemantics

import scala.collection.immutable

object TransformationActor {
  def props(t: Any => Tuple): Props = Props(new TransformationActor(t)).withDispatcher("pinned-dispatcher")
}

class TransformationActor(transform: Any => Tuple) extends Actor
  with ActorLogging
  with RequiresMessageQueue[OperatorQueueSemantics] {

  val serviceTimes: EvictingQueue[Long] = EvictingQueue.create(ConfigurationFactory.getConfig().SpecimenQueueSize)
  var router = Router(OpRoutingLogic.getLogic)

  def receive: Receive = {
    case x: Tuple =>
      process(x)
    case NewRoutee(x) =>
      doAddRoutee(x)
    case DeadRoutee(x) =>
      doRemoveRoutee(x)
    case RouteesSeq(rs) =>
      addCurrentRoutees(rs)
    case QueueVariables(mba, cva) =>
      doCalculateQoS(mba, cva)
  }

  def process(x: Tuple): Unit = {
    val time = System.currentTimeMillis
    val t = transform(x)
    serviceTimes add System.currentTimeMillis - time
    router.route(t, self)
  }

  def doAddRoutee(refs: Set[ActorRef]): Unit = {
    refs.foreach(r => router = router addRoutee r)
    log.info("Transformer - Add Routees - Size: " + router.routees.size)
  }

  def doRemoveRoutee(refs: Set[ActorRef]): Unit = {
    refs.foreach(r => {
      router = router removeRoutee r
      r ! PoisonPill //TODO: Needs a better approach! (For Keyed based, this line needs to be removed but it's needed for stateless operators)
    })
  }

  def addCurrentRoutees(rs: immutable.IndexedSeq[Routee]): Unit = {
    router = router withRoutees rs
  }

  def doCalculateQoS(mba: Long, cva: Double): Unit = {
    if (serviceTimes.size < 2) {
      sender() ! QoSMetrics(0, 0, 0)
      return
    }
    val mst = meanServiceTime
    if (mst == 0) {
      sender() ! QoSMetrics(0, 0, 0)
      return
    }
    val sdst = standardDeviationServiceTime(mst)
    val cs = coefficientOfService(mst, sdst)
    val util = utilization(mst, mba)
    val w = (util / (1-util)) * ((math.pow(cs, 2) + math.pow(cva, 2))/2) * mst
    if (w.isNaN || w.isInfinity) {
      log.info("NaN/Infinity Error in calculating WaitingTime: " +
        "\nmeanBetweenArrivals= " + mba +
        "\ncoefficientOfVariationArrival= " + cva +
        "\nmeanServiceTime= " + mst +
        "\nstandardDeviationServiceTime= " + sdst +
        "\ncoefficientOfService= " + cs +
        "\nutilization= " + util)
      sender() ! QoSMetrics(0, 0, 0)
      return
    }
    //log.info(self.path.name + " - " + w)
    //log.info("Utilization: " + Math.ceil(util*100))
    sender() ! QoSMetrics(w.asInstanceOf[Int], Math.ceil(util*100).asInstanceOf[Int], mst)
  }

  private def meanServiceTime: Long = {
    var sum: Long = 0
    val iterator = serviceTimes.iterator
    while (iterator.hasNext) {
      sum = sum + iterator.next
    }
    sum / serviceTimes.size()
  }

  private def standardDeviationServiceTime(mst: Long): Double = {
    var sum: Long = 0
    val iterator = serviceTimes.iterator
    while (iterator.hasNext) {
      sum = sum + math.pow(iterator.next - mst, 2).asInstanceOf[Long]
    }
    val temp = math.sqrt(sum / serviceTimes.size-1)
    temp
  }

  private def coefficientOfService(mst: Long, sdst: Double): Double = {
    sdst / mst
  }

  private def utilization(mst: Long, mba: Long): Double = {
    var util = mst.asInstanceOf[Double] / mba.asInstanceOf[Double]
    if (util >= 1)
      util = 0.99
    util
  }
}
