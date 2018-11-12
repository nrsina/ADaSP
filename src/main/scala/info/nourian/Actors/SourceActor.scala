package info.nourian.Actors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.dispatch.RequiresMessageQueue
import akka.routing.{ConsistentHashingRoutingLogic, Router}
import com.github.tototoshi.csv.CSVWriter
import info.nourian.Configuration.{ConfigurationFactory, OpRoutingLogic}
import info.nourian.Data.Tuple
import info.nourian.Entities._
import info.nourian.Queue.OperatorQueueSemantics

import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object SourceActor {
  def props(s: Any => Tuple): Props = Props(new SourceActor(s)).withDispatcher("pinned-dispatcher")
}

private class SourceActor(source: Any => Tuple) extends Actor
  with ActorLogging
  with RequiresMessageQueue[OperatorQueueSemantics] {

  val dateTime: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm"))
  val writer: CSVWriter = CSVWriter.open("InputRate_" + dateTime + ".csv", append = true)
  var router: Router = Router(ConsistentHashingRoutingLogic(context.system)) //Router(OpRoutingLogic.getLogic)
  var sleepTime: Int = ConfigurationFactory.getConfig().SourceMaximumMillisSleepTime
  var step: Int = -1
  val timePoint: Long = System.currentTimeMillis()

  override def preStart(): Unit = {
    writer.writeRow(List("Time", "Rate"))
  }

  def receive: Receive = {
    case StartStream => sourceFunction
    case t: Tuple =>
      process(t)
    case NewRoutee(x) =>
      doAddRoutee(x)
    case DeadRoutee(x) =>
      doRemoveRoutee(x)
  }

  private def process(tuple: Tuple): Unit = {
    router.route(tuple, sender())
  }

  def doAddRoutee(refs: Set[ActorRef]): Unit = {
    refs.foreach(r => router = router addRoutee r)
    log.info("Source - Received Add Routee Command - " + refs)
    log.info("Source - Routees Size: " + router.routees.size)
  }

  def doRemoveRoutee(refs: Set[ActorRef]): Unit = {
    refs.foreach(r => {
      router = router removeRoutee r
      r ! PoisonPill //TODO: Needs a better approach! (For Keyed based, this line needs to be removed but it's needed for stateless operators)
    })
    log.info("Source - Received Remove Routee Command - " + refs)
    log.info("Source - Routees Size: " + router.routees.size)
  }

  private def sourceFunction = {
    log.info("Starting Source Function")
    Future {
      var waitTime = System.currentTimeMillis()
      var logTime = System.currentTimeMillis()
      var throughput = 0
      while (true) {
        val t = source()
        self ! t
        throughput += 1
        if (ConfigurationFactory.getConfig().SourceLogRate && ((System.currentTimeMillis() - logTime) >= 1000)) {
          val timePassed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - timePoint)
          writer.writeRow(List(timePassed, throughput))
          throughput = 0
          logTime = System.currentTimeMillis()
        }
        TimeUnit.MILLISECONDS.sleep(new Random().nextInt(sleepTime))
        if (System.currentTimeMillis() - waitTime >= ConfigurationFactory.getConfig().WaitTimeBetweenStepsMillis) {
          sleepTime = sleepTime + step
          log.info("Source Sleep Time: " + sleepTime)
          if (sleepTime < ConfigurationFactory.getConfig().SourceMinimumMillisSleepTime || sleepTime > ConfigurationFactory.getConfig().SourceMaximumMillisSleepTime)
            step = step * -1
          waitTime = System.currentTimeMillis()
        }

      }
    }
  }
}
