package com.tfm.miguelros.anonymization

import com.tfm.miguelros.anonymization.model.{AssignedUser, User}

object Similarity {
  final val fullPoint = 10.0

  def score(a: User, b: User)(implicit args: Arguments): Double = {
    var info = 0.0

    info += manhattanDistance(a.jobRoles, b.jobRoles, 2) * fullPoint

    if (a.industryId == b.industryId) info += fullPoint

    info += manhattanDistance(a.entities, b.entities, 10) * fullPoint

    val difYears = math.abs(a.yearsExperience - b.yearsExperience)
    if (difYears <= 3) info += fullPoint - difYears

    var struct = manhattanDistance(a.relations, b.relations, 50) * fullPoint
    if (a.relations.contains(b.userId) || b.relations.contains(a.userId)) struct += 1
    struct = math.max(fullPoint, struct)

    val infoComponent = info / (4.0 * fullPoint)
    val structComponent = struct / fullPoint

    if (args.similarities.equals("info")) {
      infoComponent
    } else if (args.similarities.equals("both")) {
      0.5 * infoComponent + 0.5 * structComponent
    } else if (args.similarities.equals("struct")) {
      structComponent
    } else if (args.similarities.equals("mini")) {
      infoComponent
    } else {
      throw new Exception("Wrong similarity: [" + args.similarities + "]")
    }
  }

  def score(a: User, cluster: Seq[AssignedUser], users: Map[UserID, User])(implicit args: Arguments): Double = {
    var totalScore = 0.0
    for (u <- cluster) {
      totalScore += score(a, users(u.userId))
    }
    totalScore / cluster.size.toDouble
  }

  def fastIntersectSize(a: Set[Int], b: Set[Int]): Int = {
    if (a.size > b.size) fastIntersectSize(b, a)
    else {
      var c = 0
      for (i <- a) {
        if (b.contains(i)) c += 1
      }
      c
    }
  }

  def manhattanDistance(a: Set[Int], b:Set[Int], cutoff: Int): Double = {
    if (a.nonEmpty && b.nonEmpty) {
      val intersection = fastIntersectSize(a, b)
      val totalElem = a.size + b.size - intersection

      if (totalElem < cutoff) math.min(intersection / totalElem.toDouble, 1)
      else math.min(intersection / cutoff.toDouble, 1)

    } else {
      0.0
    }
  }
}