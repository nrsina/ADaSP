package info.nourian.GraphBuilder

import info.nourian.Configuration.ConfigurationFactory
import info.nourian.Data.Tuple
import info.nourian.Entities.WindowProps
import info.nourian.GraphBuilder.OperatorCreators.{SinkOperator, SourceOperator, TransformationOperator}

import scala.collection.mutable.ListBuffer

class GraphBuilder {

  var source: SourceOperator = _
  val transformationList: ListBuffer[TransformationOperator] = ListBuffer.empty
  var sink: SinkOperator = _
  var query: Any => Any = _
  var isRemote: Boolean = false

  def setSource(f: Any => Tuple): GraphBuilder = {
    source = new SourceOperator
    source.setOperation(f)
    this
  }

  def hasSource(): GraphBuilder = {
    source = new SourceOperator
    this
  }

  def setTransformation(f: Tuple => Tuple): GraphBuilder = {
    val op = new TransformationOperator
    op.setOperation(f)
    transformationList += op
    this
  }

  def hasTransformation(count: Int): GraphBuilder = {
    for (i <- 1 to count) {
      val op = new TransformationOperator
      transformationList += op
    }
    this
  }

  def setKeyedTransformation(f: Tuple => Tuple, winSize: Int, paneSize: Int): GraphBuilder = {
    val op = new TransformationOperator(true, WindowProps(winSize, paneSize))
    op.setOperation(f)
    transformationList += op
    this
  }

  def hasKeyedTransformation(winSize: Int, paneSize: Int, count: Int): GraphBuilder = {
    for (i <- 1 to count) {
      val op = new TransformationOperator(true, WindowProps(winSize, paneSize))
      transformationList += op
    }
    this
  }

  def setSink(f: Tuple => Any): GraphBuilder = {
    sink = new SinkOperator
    sink.setOperation(f)
    this
  }

  def hasSink(): GraphBuilder = {
    sink = new SinkOperator
    this
  }

  def setQuery(f: Any => Any): GraphBuilder = {
    query = f
    isRemote = true
    this
  }

  def runDataflow: Unit = {
    if(!isRemote) {
      val dataflow = new DataflowGraph(this)
      if (ConfigurationFactory.getConfig().DeployApproach == 0)
        dataflow.deployLocalGraph
      else if (ConfigurationFactory.getConfig().DeployApproach == 1)
        dataflow.deployRemoteGraph
    } else
      new DataflowGraph(query)
  }

}
