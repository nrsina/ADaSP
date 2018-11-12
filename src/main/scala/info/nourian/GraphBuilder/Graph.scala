package info.nourian.GraphBuilder

trait Graph {
  def deployLocalGraph: Unit
  def deployRemoteGraph: Unit
  def stopGraph
}
