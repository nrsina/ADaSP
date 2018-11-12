package info.nourian.Configuration

import java.nio.file.{Files, Paths}

import akka.routing.{RandomRoutingLogic, RoundRobinRoutingLogic, RoutingLogic, SmallestMailboxRoutingLogic}
import play.api.libs.json.{JsValue, Json}

import scala.io.Source

case class RemoteMachine(address: String, actorName: String, port: String, qosCheck: Boolean = true, initChilds: Int = 1)

object ConfigurationFactory {
  private var config: Configuration = new Configuration
  def setConfig(config: Configuration): Unit = this.config = config
  def getConfig(): Configuration = config

  def initConfigFile: Unit = {
    val confFile: String = "adasp.conf"
    implicit val machinesFormat = Json.format[RemoteMachine]
    implicit val configurationFormat = Json.format[Configuration]
    var createConfigFile = true
    if (Files.exists(Paths.get(confFile))) {
      try {
        val json: JsValue = Json.parse(Source.fromFile(confFile).getLines().mkString)
        setConfig(json.as[Configuration])
        createConfigFile = false
      } catch {
        case e: Exception => println("Cannot get config file: " + e.getMessage)
      }
    }
    if (createConfigFile) {
      try {
        Files.write(Paths.get(confFile), Json.toJson(getConfig()).toString().getBytes)
      } catch {
        case e: Exception => println("Cannot create config file: " + e.getMessage)
      }
    }
  }
}

case class Configuration (
  ActorSystemName: String = "ActorSystem",
  DeployApproach: Int = 0, //0: Local (Local Machine, Single JVM, Force Kyro Serialization), 1: Remote Machines (Remote Machines, Multiple JVM, using aeron-udp)
  RemoteTransport: String = "aeron-udp", //aeron-udp | netty-tcp | local
  DynamicThreshold: Boolean = false,
  GraphRuntimeSec: Int = 800, //0 or negative value for infinite runtime
  usePaneProcessorInKeyedOperator: Boolean = true,
  WaitTimeThreshold: Int = 1000,
  MinimumUtilizationForScaleDown: Int = 60,
  EnableFastScaleDown: Boolean = false,
  MinimumUtilForFastScaleDown: Int = 25,
  SecondsBetweenQoSCheck: Int = 3,
  MaxScaleDownNodes: Int = 5,
  ViolationUpLimit: Int = 2,
  ViolationDownLimit: Int = 3,
  SourceMaximumMillisSleepTime: Int = 60,
  SourceMinimumMillisSleepTime: Int = 1,
  SourceLogRate: Boolean = false,
  WaitTimeBetweenStepsMillis: Int = 5000,
  Routing: String = "roundrobin", //RoundRobin, SmallestMailbox, Random
  LatencyLogDurationMillis: Int = 500,
  SpecimenQueueSize: Int = 30,
  Machines: List[RemoteMachine] = List(
   RemoteMachine("127.0.0.1", "SourceSystem", "25521"),
   RemoteMachine("127.0.0.1", "TransformationSystem", "25522"),
   RemoteMachine("127.0.0.1", "SinkSystem", "25523")
  )
)

object OpRoutingLogic {
  def getLogic: RoutingLogic = {
    ConfigurationFactory.getConfig().Routing.toLowerCase() match {
      case "roundrobin"  => RoundRobinRoutingLogic()
      case "smallestmailbox"  => SmallestMailboxRoutingLogic()
      case "random"  => RandomRoutingLogic()
      case _  => RoundRobinRoutingLogic()
      //ConsistentHashingRoutingLogic(context.system)
    }
  }
}