package info.nourian.GraphBuilder.OperatorCreators

import akka.actor.{ActorRef, ActorSystem, AddressFromURIString}
import info.nourian.Actors.ControllerActor
import info.nourian.Configuration.RemoteMachine
import info.nourian.Data.Tuple
import info.nourian.Entities.Operator

class SourceOperator extends Operator[Any => Tuple] {

  protected var op: Any => Tuple = _

  def create(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef = {
    system.actorOf(
      ControllerActor.props(
        null,
        Operator.Source,
        op.asInstanceOf[Any => Any],
        qosCheck = false,
        machine.initChilds),
      name
    )
  }

  def createRemote(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef = {
    val address = AddressFromURIString(createRemoteAddress(machine))
    system.actorOf(
      ControllerActor.remoteProps(
        null,
        Operator.Source,
        address,
        qosCheck = false,
        machine.initChilds),
      name
    )
  }
}
