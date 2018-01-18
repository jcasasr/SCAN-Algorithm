package com.tfm.miguelros.anonymization

import scala.collection.mutable

object Timing {
  private val storedTime = mutable.HashMap.empty[String, (Long, Long)]

  def time[R](name: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val ms = (t1 - t0)/1000000
    println(s"xxx Elapsed time $name: $ms ms")
    result
  }

  def st[R](name: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    var us = (t1 - t0)/1000

    storedTime.get(name) match{
      case Some((times, totalTimeUs)) =>
        storedTime.put(name, (times + 1, totalTimeUs + us))
      case _ =>
        storedTime.put(name, (1, us))
    }
    result
  }

  def average(name: String): Unit = {
    val (times, totalTimeUs) = storedTime(name)
    val totalTimeMs = totalTimeUs / 1000.0
    val avg = totalTimeMs / times.toDouble
    println(s"xxx Average time $name: $avg ms - count: $times - totalTime: $totalTimeMs ms")
  }

  def reset(): Unit = {
    storedTime.clear()
  }
}