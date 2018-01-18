package com.tfm.miguelros.anonymization

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainCacheSimilarities {
  def main(args: Array[String]) {
    implicit val conf = new SparkConf().setAppName("MiguelR test")
    conf.set("spark.akka.frameSize", "32")
    conf.set("spark.default.parallelism", Config.CACHE_PARTITIONS.toString)
    conf.set("spark.speculation", "false")
    conf.set("spark.executor.cores","1")
    conf.set("spark.yarn.queue", "ros")
    conf.set("spark.yarn.executor.memoryOverhead", "2g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SimGroup], classOf[DenseMatrix[Double]]))
    implicit val arguments = Arguments.processArgs(args)

    conf.getAll.foreach{ p => println("xxx conf: " + p)}

    implicit val sc = new SparkContext(conf)
    implicit val context = new HiveContext(sc)

    val users = Datasets.getUsers(context)
    val groups = users.groupBy { user =>
      (user.region, user.disciplineId, user.careerLevel)
    }

    Timing.time("cache_similarities") {
      val g = groups.flatMap { case ((region, disciplineId, careerLevel), uig) =>
        for {
          u1 <- uig
          u2 <- uig
          if u1.userId < u2.userId
          score = Similarity.score(u1, u2)
        } yield {
          SimilarityPair(careerLevel, region, disciplineId, u1.userId, u2.userId, score)
        }
      }

      val outputName = Config.HDFS_CACHE_SIMILARITIES + "/" + arguments.similarities
      g.saveAsObjectFile(outputName)
      println("xxx stored: " + outputName)
      sc.stop()
    }

    println("xxx CACHE STORAGE OK")
  }
}
