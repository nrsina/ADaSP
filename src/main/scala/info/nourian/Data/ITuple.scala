package info.nourian.Data

import scala.collection.mutable

trait ITuple {
  def getString: String
  def getInt: Int
  def getFloat: Float
  def getDouble: Double
  def getLong: Long
  def getBoolean: Boolean
  def getSet[A]: Set[A]
  def getMap[A, B]: Map[A, B]
  def getSeq[A]: Seq[A]
  def getBuffer: mutable.Buffer[Tuple]
  def getObject[A]: A
  def getKey[K]: K
  def getAny: Any
  def clone(newVal: Any): Tuple
}
