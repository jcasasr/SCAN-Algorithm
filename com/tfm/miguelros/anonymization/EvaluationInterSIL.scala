package com.tfm.miguelros.anonymization

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object EvaluationInterSIL {
  /**
    * Total inter clusters structural loss.
    * Probability of wrongly labeling a pair of nodes (X,Y),
    * where X ∈ cl1 and Y ∈ cl2, as an edge or as an unconnected pair.
    * If we sum E(cl1, cl2) and E(cl2, cl1), no need to divide by 2 already done
    *
    * 2 * |E(cl1,cl2)| * ( 1 - (E(cl1,cl2)/ |cl1|*|cl2| ))
    */
  def interStructuralLoss(sc: SparkContext,
                          numClusters: Int,
                          relationsCount: RDD[RelationsCountEdge],
                          relationsSummary: RelationsSummaryMap): Double = {
    val totalLoss = relationsCount.map{ case ((cl1, cl2), ecl12) =>
      if (cl1 == cl2) 0
      else {
        val sizeI = relationsSummary.get(cl1)
        val sizeJ = relationsSummary.get(cl2)
        val loss = ecl12 * (1 - (ecl12 / (sizeI * sizeJ).toDouble))
        loss
      }
    }.sum
    totalLoss
  }
}
