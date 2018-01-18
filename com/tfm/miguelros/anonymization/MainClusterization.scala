package com.tfm.miguelros.anonymization

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object MainClusterization {

  def main(args: Array[String]) {
    implicit val conf = new SparkConf().setAppName("MiguelR test")
    conf.set("spark.akka.frameSize", "32")
    conf.set("spark.default.parallelism", Config.CLUSTER_PARTITIONS.toString)
    conf.set("spark.speculation", "false")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.yarn.queue", "ros")
    conf.set("spark.kryoserializer.buffer.max", "128m")
    conf.set("spark.yarn.executor.memoryOverhead", "2g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SimGroup], classOf[DenseMatrix[Double]]))
    implicit val arguments = Arguments.processArgs(args)

    implicit val sc = new SparkContext(conf)
    implicit val context = new HiveContext(sc)

    Timing.time("clusterization") {
      //retrieve generated pairs on MainCacheSimilarities
      val similaritiesName = Config.HDFS_CACHE_SIMILARITIES + "/" + arguments.similarities
      println("xxx reading: " + similaritiesName)
      val similarityPairs = sc.objectFile[SimilarityPair](similaritiesName)

      val allSimGroups = generateAllSimGroups(similarityPairs).persist(StorageLevel.MEMORY_AND_DISK_2)

      for (f <- 1 to arguments.executions) {
        println(s"xxx SimGroups count: $f --> " + allSimGroups.count())

        val futures = for (p <- 1 to Config.PARALLEL_TASKS) yield {
          Future {
            val countsGroup = allSimGroups.flatMap { simGroup =>
              val clusters = Clusterization.clusterize(simGroup)
              clusters.map { cluster =>
                (simGroup.groupDef, cluster)
              }
            }
            val clusteringResults: RDD[ClusteringResults] = countsGroup
              .zipWithIndex()
              .map { case ((groupDef, cluster), id) =>
                val remappedCluster = Cluster(id.toInt + 1, cluster.users)
                (groupDef, remappedCluster)
              }.groupBy(_._1).map { case (group, items) =>
              group -> items.map(_._2).toSeq
            }

            val path = Config.HDFS_CACHE_CLUSTERIZATION + "/" + arguments.clusteringOut + s"/${f}_$p"
            clusteringResults.saveAsObjectFile(path)
            println("xxx saving: " + path)
          }
        }
        val totalFut = Future.sequence(futures)
        Await.result(totalFut, Duration.Inf)
      }
      sc.stop()
    }
    println("xxx CLUSTERIZATION OK")
  }

  private def generateAllSimGroups(similarityPairs: RDD[SimilarityPair]) = {
    similarityPairs
      .groupBy { simPair => (simPair.careerLevel, simPair.disciplineId, simPair.region) }
      .map { case ((careerLevel, disciplineId, region), pairs) =>
        val userIndexes = (pairs.map(_.userId1).toSet ++ pairs.map(_.userId2).toSet).zipWithIndex.toMap
        val similarities = SimilarityCache.restoreSimTable(pairs, userIndexes)

        SimGroup(
          GroupDef(careerLevel, disciplineId, region),
          userIndexes,
          similarities
        )
      }
  }
}
