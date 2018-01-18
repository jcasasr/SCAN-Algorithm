package com.tfm.miguelros.anonymization

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model._

object SimilarityCache {

  def restoreSimTable(similarities: Iterable[SimilarityPair], users: Map[UserID, Int]) = {
    val table = DenseMatrix.zeros[Double](users.size, users.size)
    similarities.foreach{ simPair =>
      val u1 = users(simPair.userId1)
      val u2 = users(simPair.userId2)
      table(u1, u2) = simPair.score
      table(u2, u1) = simPair.score
    }
    table
  }
  
  def score(uid: UserID, cluster: Seq[AssignedUser], similarities: SimTable, userIndex: Map[UserID, Int]) = {
    val scores = cluster.map{ u2 =>
      val i1 = userIndex(uid)
      val i2 = userIndex(u2.userId)
      similarities(i1, i2)
    }
    val total = scores.sum
    total / cluster.size.toDouble
  }
}