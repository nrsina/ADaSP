package info.nourian.GraphBuilder

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.routing.{ActorRefRoutee, Routee}
import com.typesafe.config.{Config, ConfigFactory}
import info.nourian.Actors._
import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Entities.{StartJob, StartStream}
import info.nourian.Queries.Query

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object DataflowGraph {
  def getConfig(config: String) : Config = {
    val myConfigFile = new File("akka.conf")
    val fileConfig = ConfigFactory.parseFile(myConfigFile)
    val root = ConfigFactory.load(fileConfig)//.withFallback(ConfigFactory.load())
    root.getConfig(config)
  }
  var sourceRef: ActorRef = _
  val transformationRefList: ListBuffer[ActorRef] = ListBuffer.empty
  var sinkRef: ActorRef = _
  var monitorRef: ActorRef = _
  var controllers: immutable.IndexedSeq[Routee] = immutable.IndexedSeq.empty
}

class DataflowGraph() extends Graph {
  import DataflowGraph._
  var prevController: ActorRef = _
  var builder: GraphBuilder = _
  var system: ActorSystem = _

  def this(builder: GraphBuilder) {
    this()
    this.builder = builder
    initializeGraph
  }

  def this(f: Any => Any) {
    this()
    Query.setQuery(f)
    initializeGraph
  }

  private def initializeGraph: Unit = {
    ConfigurationFactory.initConfigFile
    val config: String = setConfigFile
    system = ActorSystem(ConfigurationFactory.getConfig().ActorSystemName, DataflowGraph.getConfig(config))
  }

  private def setConfigFile: String = {
    if (ConfigurationFactory.getConfig().DeployApproach == 0)
      return "LocalConfig"
    ConfigurationFactory.getConfig().RemoteTransport.toLowerCase() match {
      case "aeron-udp"  => "DeployConfigUDP"
      case "netty-tcp"  => "DeployConfigTCP"
      case "local" => "LocalConfig"
    }
  }

  def deployLocalGraph: Unit = {
    var machine_index = 0
    checkMachineNumbers()
    prevController = builder.source.create(system, null, "SourceController", ConfigurationFactory.getConfig().Machines(machine_index))
    machine_index += 1
    sourceRef = prevController
    for (transformer <- builder.transformationList) {
      prevController = transformer.create(system, prevController, "TransformationController-" + machine_index, ConfigurationFactory.getConfig().Machines(machine_index))
      transformationRefList += prevController
      controllers = controllers :+ ActorRefRoutee(prevController)
      machine_index += 1
    }
    prevController = builder.sink.create(system, prevController, "SinkController", ConfigurationFactory.getConfig().Machines(machine_index))
    sinkRef = prevController
    if (controllers.size > 1 && ConfigurationFactory.getConfig().DynamicThreshold)
      monitorRef = system.actorOf(CentralMonitor.props(controllers))
    startJob
  }

  def deployRemoteGraph: Unit = {
    var machine_index = 0
    checkMachineNumbers()
    prevController = builder.source.createRemote(system, prevController, "SourceController", ConfigurationFactory.getConfig().Machines(machine_index))
    machine_index += 1
    sourceRef = prevController
    for (transformer <- builder.transformationList) {
      prevController = transformer.createRemote(system, prevController, "TransformationController-" + machine_index, ConfigurationFactory.getConfig().Machines(machine_index))
      transformationRefList += prevController
      controllers = controllers :+ ActorRefRoutee(prevController)
      machine_index += 1
    }
    prevController = builder.sink.createRemote(system, prevController, "SinkController", ConfigurationFactory.getConfig().Machines(machine_index))
    sinkRef = prevController
    if (controllers.size > 1 && ConfigurationFactory.getConfig().DynamicThreshold)
      monitorRef = system.actorOf(CentralMonitor.props(controllers))
      startJob
    }

  def stopGraph: Unit = {
    /*system stop sourceRef
    for (transformer <- transformationRefList) {
      system stop transformer
    }
    system stop sinkRef
    if (monitorRef != null)
      system stop monitorRef*/
    system.terminate()
  }

  private def startJob: Unit = {
    sourceRef ! StartJob
    TimeUnit.SECONDS.sleep(1)
    for (transformer <- transformationRefList) {
      transformer ! StartJob
    }
    TimeUnit.SECONDS.sleep(1)
    sinkRef ! StartJob
    TimeUnit.SECONDS.sleep(1)
    sourceRef ! StartStream
    if (ConfigurationFactory.getConfig().GraphRuntimeSec > 0)
      startTimer
  }

  private def startTimer: Unit = {
    Future {
      val firstTime = System.currentTimeMillis()
      var continue: Boolean = true
      while (continue) {
        TimeUnit.SECONDS.sleep(1)
        if ((System.currentTimeMillis() - firstTime) >= ConfigurationFactory.getConfig().GraphRuntimeSec * 1000) {
          stopGraph
          continue = false
        }
      }
    }
  }

  private def checkMachineNumbers(): Unit = {
    val total_machines = builder.transformationList.size + 2 //2 = Source + Sink
    if (ConfigurationFactory.getConfig().Machines.size < total_machines)
      throw new Exception("Number of query tasks are higher than machines specified in the config file")
  }
}
