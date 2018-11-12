package info.nourian

import java.util.concurrent.TimeUnit

import info.nourian.GraphBuilder.GraphBuilder
import info.nourian.Data.Tuple
import info.nourian.Queries.PrimeTester

import scala.collection.mutable
import scala.util.Random

object Main extends App {

  val builder: GraphBuilder = new GraphBuilder

  def doHeavyStatefulComputation(t: Tuple): Long = ???

  def doStatefulComputation(t: Tuple): Long = ???

  builder.setSource(_ => { //Source Operator
    val data: AnyRef = readLatestFromRedis(redisURL)
    Tuple(data)
  }).setTransformation((t: Tuple) => {
    val result: Long = firstStepComputation(t)
    t.clone(result)
  }).setKeyedTransformation((t: Tuple) => { //Keyed
    var sum = 0
    try {
      val key: Int = t.getKey[Int]
      val buffer: mutable.Buffer[Tuple] = t.getBuffer
      val time = buffer.maxBy(t => t.time).time
      buffer.foreach(item => sum = sum + item.getInt)
      var result: Long = 0
      if (key == 0) //Skewness
        result = doHeavyStatefulComputation(t)
      result = doStatefulComputation(t)
      Tuple(sum, key, time)
    } catch {
      case e: Exception => println("Exception: " + e.toString)
        t.clone(-1)
    }
  }, 9, 3)
    .setSink((t: Tuple) => { //Sink Operator
    persistToDatabase(t.getLong)
    sendToOnlineAnalyzer(t.getLong)
    //Persist to database
  })
  builder.runDataflow


  val redisURL = "redisURL"

  def readLatestFromRedis(redisURL: String) = ???

  def firstStepComputation(t: Tuple) = ???
  def secondStepComputation(t: Tuple) = ???

  def persistToDatabase(getLong: Long) = ???

  def sendToOnlineAnalyzer(getLong: Long) = ???
}

//31ms sleep for one RR RM / 300 thres / 44-5 / 1 between QoS / accidentally 4 nodes in adasp.conf
//DT3 - 900 thresh / 20 - 98 - 20 ms / RR
//DT2 - 600 thresh / 17 - 90 ms / 45 - 6 time / 1 between QoS
/*
.setKeyedTransformation((t: Tuple) => {
    var sum = 0
    try {
      val buffer: mutable.Buffer[Tuple] = t.getBuffer
      buffer.foreach(item => sum = sum + item.getInt)
      TimeUnit.MILLISECONDS.sleep(new Random().nextInt(150))
      Tuple(sum, t.consistentHashKey, t.time)
    } catch {
      case e: Exception => println("Exception: " + e.toString)
      Tuple(-1, t.consistentHashKey, t.time)
    }

  }, 9, 3)
 */

/*
builder.setSource(s => {
    //TimeUnit.MILLISECONDS.sleep(new Random().nextInt(50))
    val rand = new Random()
    Tuple(rand.nextInt(50000))
  }).setTransformation((t: Tuple) => {
    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(65))
    t.clone(PrimeTester.check(t.getInt))
  }).setSink((t: Tuple) => {
    System.currentTimeMillis() - t.time
  })
 */

/*
  builder.setSource(s => {
    val rand = new Random()
    Tuple(rand.nextInt(50000))
  }).setTransformation((t: Tuple) => {
    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(20))
    PrimeTester.check(t.getInt)
    t.clone(t.getInt)
  }).setTransformation((t: Tuple) => {
    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(120))
    t.clone(PrimeTester.check(t.getInt))
  }).setSink((t: Tuple) => {
    System.currentTimeMillis() - t.time
  })
 */

/*
  builder.setQuery(tuple => {
    val t: Tuple = tuple.asInstanceOf[Tuple]
    var sum = 0
    try {
      val key: Int = t.getKey[Int]
      val buffer: mutable.Buffer[Tuple] = t.getBuffer
      val time = buffer.maxBy(t => t.time).time
      buffer.foreach(item => sum = sum + item.getInt)
      if (key == 0)
        TimeUnit.MILLISECONDS.sleep(new Random().nextInt(95))
      TimeUnit.MILLISECONDS.sleep(new Random().nextInt(21))
      Tuple(sum, key, time)
      //t.clone(sum)
    } catch {
      case e: Exception => println("Exception: " + e.toString)
        t.clone(-1)
    }
  })
  */