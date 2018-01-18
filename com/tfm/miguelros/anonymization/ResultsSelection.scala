package com.tfm.miguelros.anonymization

import model.ResultsClusterEval

object ResultsSelection {
  def normalizeResults(results: Seq[ResultsClusterEval])(implicit arguments: Arguments): Seq[ResultsClusterEval] = {
    val contacts = normalize(results.map{ x => x.clusterSim.contacts.ratio})
    val jobRoles = normalize(results.map{ x => x.clusterSim.jobRoles.ratio})
    val entities = normalize(results.map{ x => x.clusterSim.entities.ratio})
    val industries = normalize(results.map{ x => x.clusterSim.industries.ratio})
    val disciplines = normalize(results.map{ x => x.clusterSim.disciplines.ratio})

    val values = if (arguments.similarities.equals("info")) {
      Seq(jobRoles, entities, disciplines, industries)
    } else if (arguments.similarities.equals("both")) {
      Seq(contacts, jobRoles, entities, disciplines, industries)
    } else if (arguments.similarities.equals("struct")) {
      Seq(contacts)
    } else if (arguments.similarities.equals("mini")) {
      Seq(contacts)
    } else {
      throw new Exception("Wrong similarity: [" + arguments.similarities + "]")
    }

    for (i <- results.indices) yield {
      val damageScore = values.map{ vs =>
        math.min(Double.MaxValue, vs(i))
      }.product

      val r = results(i)
      r.copy(damageScore = damageScore)
    }
  }

  def selectBest(normalizedResults: Seq[ResultsClusterEval]): ResultsClusterEval = {
    normalizedResults.minBy(_.damageScore)
  }

  private def normalize(values: Seq[Double]): Seq[Double] = {
    val max = values.max
    if (max > 0) values.map{ v => v / max}
    else values.map{ v => 0.0 }
  }
}
