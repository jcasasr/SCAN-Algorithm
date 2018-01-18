package com.tfm.miguelros.anonymization

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainUsersBasicStats {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MiguelR test")
    //conf.set("spark.storage.memoryFraction", "0.2") //0.6 ?
    conf.set("spark.akka.frameSize", "32")
    conf.set("spark.default.parallelism", Config.EVAL_PARTITIONS.toString)
    conf.set("spark.speculation", "true")
    conf.set("spark.speculation.interval", "1s")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.yarn.queue", "ros")
    conf.set("spark.yarn.executor.memoryOverhead", "2g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SimGroup], classOf[DenseMatrix[Double]]))

    implicit val sc = new SparkContext(conf)
    implicit val context = new HiveContext(sc)

    val users = Datasets.getUsers(context)

    println("xxx USers count: " + users.count())
    println("xxx num different entities=" + users.flatMap(_.entities).groupBy(identity).keys.count)

    var statCounter = users.map(_.entities.size).stats()
    println("xxx Entities: mean= " + statCounter.mean + " - stdef = " + statCounter.stdev)
    println("xxx num different entities=" + users.flatMap(_.entities).groupBy(identity).keys.count)

    statCounter = users.map(_.jobRoles.size).stats()
    println("xxx Job roles: mean= " + statCounter.mean + " - stdef = " + statCounter.stdev)
    println("xxx job roles entities=" + users.flatMap(_.jobRoles).groupBy(identity).keys.count)

    statCounter = users.map(_.relations.size).stats()
    println("xxx relations: mean= " + statCounter.mean + " - stdef = " + statCounter.stdev)

    val totalRels = users.map(_.relations.size).sum()
    println("xxx relations: sum= " + totalRels / 2.0)

    println("xxx disciplines: " + users.map(_.disciplineId).groupBy(identity).keys.count)
    println("xxx industry: " + users.map(_.industryId).groupBy(identity).keys.count)
    println("xxx region: " + users.map(_.region).groupBy(identity).keys.count)
    println("xxx years exp: " + users.map(_.yearsExperience).groupBy(identity).keys.count)
    println("xxx years exp: " + users.map(_.yearsExperience).groupBy(identity).keys.count)

    val sum = users.map(_.relations.size).sum
    println("Total sum : " + sum)
    sc.stop()

  }
}

