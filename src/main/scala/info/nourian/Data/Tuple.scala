package info.nourian.Data

import akka.routing.ConsistentHashingRouter.ConsistentHashable

import scala.collection.mutable

case class Tuple(value: Any, key: Any = 0, time: Long = System.currentTimeMillis)
  extends ITuple
    with ConsistentHashable {

  def getString: String = {
    value.asInstanceOf[String]
  }

  def getInt: Int = {
    value.asInstanceOf[Int]
  }

  def getFloat: Float = {
    value.asInstanceOf[Float]
  }

  def getDouble: Double = {
    value.asInstanceOf[Double]
  }

  def getLong: Long = {
    value.asInstanceOf[Long]
  }

  def getBoolean: Boolean = {
    value.asInstanceOf[Boolean]
  }

  def getSet[A]: Set[A] = {
    value.asInstanceOf[Set[A]]
  }

  def getMap[A, B]: Map[A, B] = {
    value.asInstanceOf[Map[A, B]]
  }

  def getSeq[A]: Seq[A] = {
    value.asInstanceOf[Seq[A]]
  }

  def getBuffer: mutable.Buffer[Tuple] = {
    value.asInstanceOf[mutable.Buffer[Tuple]]
  }

  def getObject[A]: A = {
    value.asInstanceOf[A]
  }

  def getAny: Any = {
    value
  }

  def getKey[K]: K = {
    key.asInstanceOf[K]
  }

  def clone(newVal: Any): Tuple = Tuple(newVal, key, time)

  override def consistentHashKey: Any = key
}
