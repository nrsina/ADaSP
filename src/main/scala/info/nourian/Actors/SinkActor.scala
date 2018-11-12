package info.nourian.Actors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, Timers}
import com.github.tototoshi.csv.CSVWriter
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple
import info.nourian.Entities.{CalculateStats, TickKey}

import scala.concurrent.duration._

object SinkActor {
  def props(s: Tuple => Any): Props = Props(new SinkActor(s)).withDispatcher("pinned-dispatcher")
}

private class SinkActor(sink: Tuple => Any) extends Actor
  with ActorLogging
  with Timers{

  val dateTime: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm"))
  val writer: CSVWriter = CSVWriter.open("LatencyThroughput_" + dateTime + ".csv", append = true)
  var throughput: Int = 0
  var latency: Long = 0
  var timePoint: Long = 0

  override def preStart(): Unit = {
    writer.writeRow(List("Time", "Latency"))
    timers.startSingleTimer(TickKey, CalculateStats, ConfigurationFactory.getConfig().LatencyLogDurationMillis millisecond)
    timePoint = System.currentTimeMillis()
  }

  override def postStop(): Unit = {
    writer.close
  }

  def receive: Receive = {
    case t: Tuple =>
      doSink(t)
    case CalculateStats =>
      doCalculateStats()
  }


  def doSink(t : Tuple): Unit = {
    val time: Long = sink(t).asInstanceOf[Long]
    latency += time
    throughput += 1
  }

  def doCalculateStats(): Unit = {
    if (throughput >= 1 && latency != 0) {
      val timePassed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - timePoint)
      writer.writeRow(List(timePassed, latency/throughput))
      throughput = 0
      latency = 0
    }
    timers.startSingleTimer(TickKey, CalculateStats, ConfigurationFactory.getConfig().LatencyLogDurationMillis millisecond)
  }
}