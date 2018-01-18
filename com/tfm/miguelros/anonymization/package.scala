package com.tfm.miguelros

import breeze.linalg.DenseMatrix
import com.tfm.miguelros.anonymization.model.{Cluster, GroupDef}
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap

package object anonymization {
  type UserID = Int
  type ClusterID = Int
  type ClusterUniqueID = Int

  type SimTable = DenseMatrix[Double]

  type RelationsCountEdge = ((ClusterID, ClusterID), Int)
  type RelationsSummaryMap = Int2IntLinkedOpenHashMap

  type ClusteringResults = (GroupDef, Seq[Cluster])

  type UserIDToClusterIDMap = Int2IntLinkedOpenHashMap
  type ClusterIDToUserIDsMap = Map[ClusterID, Array[UserID]]
}
