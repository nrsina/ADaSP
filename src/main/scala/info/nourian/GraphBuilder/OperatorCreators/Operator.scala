package info.nourian.GraphBuilder.OperatorCreators

import akka.actor.{ActorRef, ActorSystem}
import info.nourian.Configuration.{ConfigurationFactory, RemoteMachine}

trait Operator[O] {

  protected var op: O

  def getOperation: O = op

  def setOperation(o: O): Unit = op = o

  def create(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef

  def createRemote(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef

  def createRemoteAddress(machine: RemoteMachine): String = {
    var protocol: String = "akka.tcp://"
    ConfigurationFactory.getConfig().RemoteTransport.toLowerCase() match {
      case "aeron-udp"  => protocol = "akka://"
      case "netty-tcp"  => protocol = "akka.tcp://"
    }
    protocol + machine.actorName + "@" + machine.address + ":" + machine.port
  }

}
