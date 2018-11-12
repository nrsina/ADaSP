package info.nourian.Queries

object Query {

  private var f: Any => Any = _

  def getQuery: Any => Any = f

  def setQuery(f: Any => Any): Unit = this.f = f
}
