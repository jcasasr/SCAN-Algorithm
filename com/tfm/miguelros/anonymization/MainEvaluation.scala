package com.tfm.miguelros.anonymization

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MainEvaluation {

  def main(args: Array[String]) {
    implicit val conf = new SparkConf(true).setAppName("MiguelR test")
    conf.set("spark.akka.frameSize", "32")
    conf.set("spark.default.parallelism", Config.EVAL_PARTITIONS.toString)
    conf.set("spark.speculation", "true")
    conf.set("spark.speculation.interval", "1s")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.yarn.queue", "ros")
    conf.set("spark.yarn.executor.memoryOverhead", "2g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SimGroup], classOf[DenseMatrix[Double]]))
    implicit val arguments = Arguments.processArgs(args)

    implicit val sc = new SparkContext(conf)
    implicit val context = new HiveContext(sc)

    val evaluationResultsB = Seq.newBuilder[ResultsClusterEval]

    Timing.time("evaluation") {
      val allGroupedUsers = getGroupedUsers(context).persist(StorageLevel.MEMORY_AND_DISK_2)

      val numClusters = readNumClusters(allGroupedUsers)

      for (f <- 1 to arguments.executions) {
        println(s"xxx allGroupedUsers count: $f --> " + allGroupedUsers.count())

        val futures = for (p <- 1 to Config.PARALLEL_TASKS) yield {
          Future {
            val name = s"${f}_$p"
            val clustering = readClusteringResults(f, p)
            val clustersFullInfo = buildClusterFullInfo(allGroupedUsers, clustering)


            val clusterSim = clustersFullInfo.map { cluster =>
              val similarity = EvaluationClusterSimilarity.clusterSimilarity(numClusters, cluster)
              similarity
            }.reduce(_ + _)

            ResultsClusterEval(name, 0.0, clusterSim)
          }
        }
        val totalFut = Future.sequence(futures)
        val results = Await.result(totalFut, Duration.Inf)
        evaluationResultsB ++= results
      }
      sc.stop()
    }

    val evaluationResults = evaluationResultsB.result()
    val normalizedResults = ResultsSelection.normalizeResults(evaluationResults)
    normalizedResults.foreach{ r => println("xxx result: " + r)}
    println("\n\n\n")

    val best = ResultsSelection.selectBest(normalizedResults)
    printBest(best)
    println("xxx EVALUATION OK")
  }

  def buildClusterFullInfo(allGroupedUsers: RDD[(GroupDef, Iterable[(UserID, User)])],
                           clustering: Array[(GroupDef, Seq[Cluster])]): RDD[ClusterFull] = {
    allGroupedUsers.flatMap { case (groupDef, users) =>
      val usersMap = users.toMap
      val clusters = clustering.find { g => g._1 == groupDef }.map(_._2).getOrElse(Seq.empty)

      clusters.map { c =>
        val users: Map[UserID, User] = c.users.map { assigned =>
          assigned.userId -> usersMap(assigned.userId)
        }(collection.breakOut)
        ClusterFull(c.clusterId, groupDef, users)
      }
    }
  }

  private def getGroupedUsers(context: HiveContext) = {
    Datasets.getUsers(context).groupBy { u =>
      (u.careerLevel, u.disciplineId, u.region)
    }.map { case (group, users) =>
      GroupDef(group._1, group._2, group._3) ->
        users.map { u => u.userId -> u }
    }
  }

  def buildClusterRelations(clustersFullInfo: RDD[ClusterFull],
                            clusterMap: UserIDToClusterIDMap,
                            numClusters: Int): (RDD[RelationsCountEdge], RelationsSummaryMap) = {

    val relationsCountRDD: RDD[RelationsCountEdge] = clustersFullInfo
      .flatMap { cluster =>
        cluster.users.values.flatMap { user =>
          user.relations.toIterator.map { uid2 =>
           ((clusterMap.get(user.userId), clusterMap.get(uid2)), 1)
          }
        }
      }.filter{ case ((cl1, cl2), count) => cl1 != 0 && cl2 != 0 && cl1 < cl2}
        .flatMap{ case ((cl1, cl2), count) =>
          Seq(((cl1, cl2), count), ((cl2, cl1), count))
        }
      .reduceByKey(_ + _)

    val summariesMapArray = relationsCountRDD.map{ case ((cl1, cl2), count) =>
      cl1 -> count
    }.reduceByKey(_ + _).collect()

    val summariesMap = new RelationsSummaryMap(numClusters)
    summariesMap.defaultReturnValue(0)

    summariesMapArray.foreach { case (cl1, count) =>
      summariesMap.put(cl1, count)
    }
    println("xxx summariesMap table size: " + summariesMap.size)
    println("xxx max entry: " + summariesMapArray.maxBy(_._2))

    (relationsCountRDD, summariesMap)
  }

  private def printBest(result: ResultsClusterEval) = {
    println("xxx best: " + result)

    val cs = result.clusterSim
    println("xxx contacts ratio: " + cs.contacts.ratio)
    println("xxx entities ratio " + cs.entities.ratio)
    println("xxx disciplines ratio: " + cs.disciplines.ratio)
    println("xxx industries ratio: " + cs.industries.ratio)
    println("xxx job roles ratio: " + cs.jobRoles.ratio)
  }

  private def readNumClusters(allGroupedUsers: RDD[(GroupDef, Iterable[(UserID, User)])])(implicit sc: SparkContext, args: Arguments): Int = {
    val clustering = readClusteringResults(1, 1)
    val clustersFullInfo = buildClusterFullInfo(allGroupedUsers, clustering)
    clustersFullInfo.count().toInt
  }

  private def readClusteringResults(f: Int, p: Int)(implicit sc: SparkContext, args: Arguments): Array[(GroupDef, Seq[Cluster])] = {
    assert(f > 0)
    assert(p > 0)

    val name = s"${f}_$p"
    val path = Config.HDFS_CACHE_CLUSTERIZATION + "/" + args.clusteringOut + "/" + name
    sc.objectFile[ClusteringResults](path).collect()
  }
}
