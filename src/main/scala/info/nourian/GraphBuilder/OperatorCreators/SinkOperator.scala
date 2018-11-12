package info.nourian.GraphBuilder.OperatorCreators

import akka.actor.{ActorRef, ActorSystem, AddressFromURIString}
import info.nourian.Actors.ControllerActor
import info.nourian.Configuration.RemoteMachine
import info.nourian.Data.Tuple
import info.nourian.Entities.Operator

class SinkOperator extends Operator[Tuple => Any] {

  protected var op: Tuple => Any = _

  def create(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef = {
    system.actorOf(
      ControllerActor.props(
        prevController,
        Operator.Sink,
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
        prevController,
        Operator.Sink,
        address,
        qosCheck = false,
        machine.initChilds),
      name
    )
  }
}
