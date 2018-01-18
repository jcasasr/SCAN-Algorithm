package com.tfm.miguelros.anonymization

import com.tfm.miguelros.anonymization.model.{ClusterFull, ResultsClusterSim, ResultsSum, User}
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap

object EvaluationClusterSimilarity {
  def clusterSimilarity(totalClusters: Int, cl: ClusterFull): ResultsClusterSim = {
    val sharedContacts = uniqueValues(cl, { user =>
      user.relations.toSeq :+ user.userId
    })
    val sharedDisciplines = numDiffValues(cl, { user =>
      Seq(user.disciplineId)
    })
    val sharedEntities = numDiffValues(cl, { user =>
      user.entities.toSeq
    })
    val sharedJobRoles = numDiffValues(cl, { user =>
      user.jobRoles.toSeq
    })
    val sharedIndustries = numDiffValues(cl, { user =>
      Seq(user.industryId)
    })

    ResultsClusterSim(
      totalClusters = totalClusters,
      contacts = sharedContacts,
      jobRoles = sharedJobRoles,
      entities = sharedEntities,
      disciplines = sharedDisciplines,
      industries = sharedIndustries
    )
  }

  private def uniqueValues(cl: ClusterFull, f: User => Seq[Int]): ResultsSum = {
    var total = 0
    var shared = 0
    val items = new Int2IntLinkedOpenHashMap
    items.defaultReturnValue(0)

    cl.users.foreach { case (uid, user) =>
      val keys = f(user)
      for(k <- keys) {
        total += 1
        increment(items, k)
      }
    }

    val unique = items.int2IntEntrySet().size()
    ResultsSum(value = unique, total = total)
  }

  private def numDiffValues(cl: ClusterFull, f: User => Seq[Int]): ResultsSum = {
    var total = 0
    val items = new Int2IntLinkedOpenHashMap
    items.defaultReturnValue(0)

    cl.users.foreach { case (uid, user) =>
      val keys = f(user)
      for(k <- keys) {
        total += 1
        increment(items, k)
      }
    }
    ResultsSum(value = items.size, total = 1)
  }

  private def increment(map: Int2IntLinkedOpenHashMap, key: Int): Unit = {
    val v = map.get(key)
    map.put(key, v + 1)
  }
}
