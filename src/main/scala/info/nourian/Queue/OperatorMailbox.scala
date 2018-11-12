package info.nourian.Queue

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.dispatch.{Envelope, MailboxType, MessageQueue, ProducesMessageQueue}
import com.google.common.collect.EvictingQueue
import com.typesafe.config.Config
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple
import info.nourian.Entities._

import scala.compat.Platform.ConcurrentModificationException

trait OperatorQueueSemantics

object OperatorMailbox {
  class OperatorQueue extends MessageQueue
    with OperatorQueueSemantics {

    private final val tupleQueue = new ConcurrentLinkedQueue[Envelope]
    private final val controlQueue = new ConcurrentLinkedQueue[Envelope]
    private final val specimenQueue: EvictingQueue[Long] = EvictingQueue.create(ConfigurationFactory.getConfig().SpecimenQueueSize)
    private var time: Long = System.currentTimeMillis

    def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      handle.message match {
        case _: Tuple =>
          tupleQueue offer handle
          val temp = System.currentTimeMillis
          try {
            specimenQueue add temp - time
          } catch {
            case e: Exception =>
              println("Exception in adding message to mailbox: " + e.getMessage)
          }
          time = temp
        case PoisonPill =>
          tupleQueue offer handle
        case SendQoSData =>
          var mba: Long = 0
          var cva: Double = 0
          if (specimenQueue.size >= 2) {
            mba = meanBetweenArrivals
            val sdba = standardDeviationBetweenArrivals(mba)
            cva = coefficientOfVariationArrival(mba, sdba)
          }
          val newEnvelope = handle.copy(QueueVariables(mba, cva), handle.sender)
          controlQueue offer newEnvelope
        case _ =>
          controlQueue offer handle
      }
    }

    def dequeue(): Envelope = {
      if (!controlQueue.isEmpty)
        controlQueue.poll
      else
        tupleQueue.poll
    }

    def numberOfMessages: Int = tupleQueue.size + controlQueue.size

    def hasMessages: Boolean = !tupleQueue.isEmpty || !controlQueue.isEmpty

    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      while (hasMessages) {
        deadLetters.enqueue(owner, dequeue())
      }
    }
    private def meanBetweenArrivals: Long = {
      var sum: Long = 0
      val iterator = specimenQueue.iterator
      try {
        while (iterator.hasNext) {
          sum = sum + iterator.next
        }
      } catch {
        case e: ConcurrentModificationException =>
          println("ConcurrentModification in iterating sample queue of mailbox")
      }
      if (sum < specimenQueue.size)
        1
      else
        sum / specimenQueue.size
    }

    private def standardDeviationBetweenArrivals(mba: Long): Double = {
      var sum: Long = 0
      val iterator = specimenQueue.iterator
      try {
        while (iterator.hasNext) {
          sum = sum + math.pow(iterator.next - mba, 2).asInstanceOf[Long]
        }
      } catch {
        case e: ConcurrentModificationException =>
          println("ConcurrentModification in iterating sample queue of mailbox")
      }
      if (sum < specimenQueue.size - 1)
        1
      else
        math.sqrt(sum / (specimenQueue.size - 1))
    }

    private def coefficientOfVariationArrival(mba: Long, sdba: Double): Double = {
      sdba / mba
    }
  }
}

class OperatorMailbox extends MailboxType with ProducesMessageQueue[OperatorMailbox.OperatorQueue] {
  import OperatorMailbox._

  def this(settings: ActorSystem.Settings, config: Config) = {
    // put your initialization code here
    this
  }
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new OperatorQueue()
}
