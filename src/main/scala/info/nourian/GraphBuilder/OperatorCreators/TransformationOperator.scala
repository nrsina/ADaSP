package info.nourian.GraphBuilder.OperatorCreators

import akka.actor.{ActorRef, ActorSystem, AddressFromURIString, Props}
import info.nourian.Actors.{ControllerActor, StatefulController}
import info.nourian.Configuration.RemoteMachine
import info.nourian.Data.Tuple
import info.nourian.Entities.{Operator, WindowProps}

class TransformationOperator(keyed: Boolean, winProps: WindowProps) extends Operator[Tuple => Tuple] {

  protected var op: Tuple => Tuple = _

  def this() =
    this(keyed = false, winProps = null)

  def create(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef = {
    var props: Props = null
    var actorName = name
    if (keyed) {
      props = StatefulController.props(
        prevController,
        Operator.StatefulTransformation,
        op.asInstanceOf[Any => Any],
        winProps,
        machine.qosCheck,
        machine.initChilds)
      actorName = "Keyed" + name
    } else
      props = ControllerActor.props(
        prevController,
        Operator.Transformation,
        op.asInstanceOf[Any => Any],
        machine.qosCheck,
        machine.initChilds)

    system.actorOf(props, actorName)

  }

  def createRemote(system: ActorSystem, prevController: ActorRef, name: String, machine: RemoteMachine): ActorRef = {
    val address = AddressFromURIString(createRemoteAddress(machine))
    var props: Props = null
    var actorName = name
    if (keyed) {
      props = StatefulController.remoteProps(
        prevController,
        Operator.StatefulTransformation,
        address,
        winProps,
        machine.qosCheck,
        machine.initChilds)
      actorName = "Keyed" + name
    } else
      props = ControllerActor.remoteProps(
        prevController,
        Operator.Transformation,
        address,
        machine.qosCheck,
        machine.initChilds)

    system.actorOf(props, actorName)
  }
}
