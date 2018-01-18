package com.tfm.miguelros.anonymization

import model._

object EvaluationIntraSIL {
  /**
    * Total intra-cluster structural loss intraSIL
    * is the probability of wrongly
    * labeling a pair of nodes in cl as an edge or as an unconnected pair.
    * There are |Ecl| edges, and (|cl| over two) - |Ecl| pairs of unconnected nodes
    */
  def intraStructuralLoss(cl: ClusterFull): Double = {
    val clOverTwo = nOverTwo(cl.users.size)
    val ecl = numIntraEdges(cl)
    2 * ecl * (1 - ecl / clOverTwo.toDouble)
  }

  /**
    * Counts the number of unique edges between users of the given cluster
    */
  private def numIntraEdges(cluster: ClusterFull): Int = {
    val edges = Set.newBuilder[(UserID, UserID)]
    cluster.users.foreach { case (uid, user) =>
      user.relations
        .foreach { r =>
          if (cluster.users.contains(r)) {
            val left = math.min(uid, r)
            val right = math.max(uid, r)
            val edge = (left, right)
            edges += edge
          }
        }
    }
    edges.result().size
  }

  private def nOverTwo(n: Int) = {
    fact(n) / (2 * fact(n - 2))
  }

  private def fact(n: Int): Int = if (n <= 1) 1 else n * fact(n - 1)

}
