package com.tfm.miguelros.anonymization

import com.tfm.miguelros.anonymization.model._

import scala.collection.mutable

object Clusterization {

  def clusterize(simGroup: SimGroup)(implicit args: Arguments): Seq[Cluster] = {
    println("xxx clusterizing... " + simGroup.groupDef)
    Timing.time("clusterize") {
      var allUsers = scala.util.Random.shuffle(simGroup.users.keys)
      val usedUsers = mutable.HashSet.empty[UserID]

      var cluster = new mutable.ArrayBuffer[AssignedUser](args.k * 2)
      var clusterId = 0
      val clusters = Seq.newBuilder[Cluster]

      while (usedUsers.size < allUsers.size) {
        if (cluster.isEmpty) {
          //pick random user, just the first item as it is shuffled already
          val userId = pickFirstUser(allUsers, usedUsers)
          cluster += AssignedUser(userId, -1.0)
          usedUsers.add(userId)

        } else {
          val matched = findBestMatch(allUsers, usedUsers, simGroup.users, simGroup.similarities, cluster)
          cluster += matched
          usedUsers.add(matched.userId)
        }

        if (cluster.size == args.k) {
          clusterId += 1
          clusters += Cluster(clusterId, cluster.result.toSet)
          cluster.clear()
        }
      }

      val allClusters = clusters.result.toArray
      if (cluster.size < args.k) {
        val newLast = allClusters.last.copy(users = allClusters.last.users ++ cluster)
        allClusters.update(allClusters.length - 1, newLast)
      }
      allClusters.toSeq
    }
  }

  private def pickFirstUser(allUsers: Iterable[UserID], usedUsers: mutable.HashSet[UserID]): UserID = {
    allUsers.collectFirst { case userId if !usedUsers.contains(userId) =>
      userId
    }.get
  }

  private def findBestMatch(allUsers: Iterable[UserID],
                            usedUsers: mutable.HashSet[UserID],
                            userIndex: Map[UserID, Int],
                            similarities: SimTable,
                            cluster: mutable.ArrayBuffer[AssignedUser]): AssignedUser = {
    val topScores = topNs(allUsers, usedUsers, userIndex, similarities, cluster, Config.NUM_TOP_RAND)
    println("xxx top scores for: " + cluster + " - " + topScores)
    biasedRandomPick(topScores)
  }

  private def biasedRandomPick(userWeights: Seq[AssignedUser]): AssignedUser = {
    val total = userWeights.foldLeft(0.0) { (a, b) => a + b.score }
    val chosenWeight = scala.util.Random.nextDouble() * total

    val acums = userWeights.foldLeft(0.0, List[Double]()) { (ac, i) => (i.score + ac._1, i.score + ac._1 :: ac._2) }._2.reverse.zipWithIndex
    val idxOpt = acums.find(_._1 >= chosenWeight)
    if (idxOpt.isEmpty) {
      throw new Exception("Idx opt empty. " + userWeights + " -- " + total + " --- " + chosenWeight + " -- " + acums)
    }
    val assignedUser = userWeights(idxOpt.get._2)
    assignedUser
  }

  def topNs(allUsers: Iterable[UserID],
            usedUsers: mutable.HashSet[UserID],
            userIndex: Map[UserID, Int],
            similarities: SimTable,
            cluster: mutable.ArrayBuffer[AssignedUser],
            n: Int): Seq[AssignedUser] = {
    var ss = List[AssignedUser]()
    var min = Double.MaxValue
    var len = 0

    for (uid <- allUsers) {
      if (!usedUsers.contains(uid)) {
        val score = SimilarityCache.score(uid, cluster, similarities, userIndex)
        val el = AssignedUser(uid, score)
        if (len < n || score > min) {
          ss = (el :: ss).sortBy(_.score)
          min = ss.head.score
          len += 1
        }
        if (len > n) {
          ss = ss.tail
          min = ss.head.score
          len -= 1
        }
      }
    }
    ss
  }
}