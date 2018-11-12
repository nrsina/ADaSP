package info.nourian.Actors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, PoisonPill, Props}
import akka.dispatch.RequiresMessageQueue
import akka.routing.{ActorRefRoutee, ConsistentHashingRoutingLogic, Routee, Router}
import info.nourian.Data.Tuple
import info.nourian.Entities._
import info.nourian.Queue.OperatorQueueSemantics

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.util.Random

object KeyedTransformation {
  def props[K](t: Any => Tuple, winProps: WindowProps): Props = Props(new KeyedTransformation[K](t, winProps)).withDispatcher("pinned-dispatcher")
}

class KeyedTransformation[K](func: Any => Tuple, winProps: WindowProps) extends TransformationActor(func)
  with ActorLogging
  with RequiresMessageQueue[OperatorQueueSemantics] {

  val actorName: String = self.path.name + " - "
  val keyMap: mutable.HashMap[K, mutable.Buffer[Tuple]] = mutable.HashMap.empty
  val paneMap: mutable.HashMap[K, mutable.Buffer[Tuple]] = mutable.HashMap.empty
  val logic = ConsistentHashingRoutingLogic(context.system)
  var keyedRouter = Router(logic)
  var shutdown: Boolean = false

  override def preStart(): Unit = {
    val routees: immutable.IndexedSeq[Routee] = immutable.IndexedSeq(ActorRefRoutee(self))
    keyedRouter = keyedRouter withRoutees routees
    //func = func.asInstanceOf[Any => Tuple]
  }

  override def receive: Receive = {
    case t: Tuple =>
      if (shutdown)
        keyedRouter route (t, sender())
      else {
        if (winProps.enablePane)
          process(t)
        else
          processWindow(t)
      }
    case Pane(p) =>
      processWindow(p)
    case NewRoutee(x) =>
      doAddRoutee(x)
    case DeadRoutee(x) =>
      doRemoveRoutee(x)
    case RouteesSeq(rs) =>
      addCurrentRoutees(rs)
    case QueueVariables(mba, cva) =>
      doCalculateQoS(mba, cva)
    case KeyState(key, list) =>
      //doAddKeyStates(key.asInstanceOf[K], list)
      doAddStates(key.asInstanceOf[K], keyMap, list)
    case PaneState(key, list) =>
      //doAddPaneStates(key.asInstanceOf[K], list)
      doAddStates(key.asInstanceOf[K], paneMap, list)
    case NotifyScaled(rs) =>
      log info actorName + "NotifyScaled has been received"
      keyedRouter = keyedRouter withRoutees rs
      syncKeyStates()
    case GracefulShutdown =>
      log info actorName + "GracefulShutdown has been received"
      shutdown = true
      syncPaneStates()
      self ! PoisonPill
  }

  override def process(tuple: Tuple): Unit = {
    val time = System.currentTimeMillis
    val key = tuple.getKey[K]
    val panes = paneMap.getOrElseUpdate(key, null)
    if (panes == null)
      paneMap put (key, mutable.Buffer(tuple))
    else {
      panes += tuple
      if (panes.size >= winProps.paneSize) {
        //log info actorName + "Closing pane with key: " + key
        val pane = func(Tuple(panes.dropRight(panes.size - winProps.paneSize), key))
        panes.remove(0, winProps.paneSize)
        keyedRouter route (Pane(pane), self)
        val elapsed = System.currentTimeMillis - time
        serviceTimes add elapsed
      }
    }
  }

  def processWindow(tuple: Tuple): Unit = {
    val time = System.currentTimeMillis
    val key = tuple.getKey[K]
    //TODO: is checking needed?
    val rt = logic select (tuple, keyedRouter.routees)
    if (rt != ActorRefRoutee(self)) {
      if (winProps.enablePane)
        keyedRouter route (Pane(tuple), self)
      else
        keyedRouter route (tuple, self)
    }
    var list = keyMap.getOrElseUpdate(key, null)
    if (list == null)
      keyMap put (key, mutable.Buffer(tuple))
    else {
      list += tuple
      if (list.size >= winProps.winSize) {
        //log info actorName + "Closing window with key: " + key
        val result = func(Tuple(list.dropRight(list.size - winProps.winSize), key))
        list.remove(0, winProps.winSize)
        router route (result, self)
        val elapsed = System.currentTimeMillis - time
        serviceTimes add elapsed
      }
    }

  }

  def syncKeyStates(): Unit = {
    val movedStates : ListBuffer[K] = ListBuffer.empty
    keyMap foreach (entry => {
      val state: Control = KeyState(entry._1, entry._2.toList)
      val rt = logic select (state, keyedRouter.routees)
      if (rt != ActorRefRoutee(self)) {
        keyedRouter route (state, self)
        movedStates += entry._1
        //log info actorName + "Sending Key State " + entry._1 + " to: " + rt
      }
    })
    movedStates foreach (_ => keyMap remove _)
  }

  def syncPaneStates(): Unit = {
    val movedStates : ListBuffer[K] = ListBuffer.empty
    paneMap foreach (entry => {
      val state: Control = PaneState(entry._1, entry._2.toList)
      val rt = logic select (state, keyedRouter.routees)
      if (rt != ActorRefRoutee(self)) {
        keyedRouter route (state, self)
        movedStates += entry._1
        //log info actorName + "Sending Pane State " + entry._1 + " to: " + rt
      }
    })
    movedStates foreach (_ => paneMap remove _)
  }

  def doAddStates(key: K, map: mutable.HashMap[K, mutable.Buffer[Tuple]], value: List[Tuple]): Unit = {
    //log info actorName + "Adding State from " + sender() + " with key: " + key
    var list = map.getOrElseUpdate(key, null)
    if (list == null)
      map put (key, value.toBuffer)
    else {
      list ++= value
      //map update(key, list)
    }
  }

  def testFunc(t: Tuple): Tuple = {
    val buffer: mutable.Buffer[Tuple] = t.getBuffer
    var sum = 0
    buffer.foreach(item => sum = sum + item.getInt)
    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(40))
    Tuple(sum, t.consistentHashKey, t.time)
  }


  /*
  def doAddKeyStates(key: K, value: List[Tuple]): Unit = {
    var list = keyMap.getOrElseUpdate(key, null)
    if (list == null)
      keyMap put (key, value.toBuffer)
    else {
      list ++= value
      //keyMap update(key, list)
    }
  }

  def doAddPaneStates(key: K, panes: List[Tuple]): Unit = {
    var list = paneMap.getOrElseUpdate(key, null)
    if (list == null)
      paneMap put (key, panes.toBuffer)
    else {
      list ++= panes
      //paneMap update(key, list)
    }
  }
  */
}
