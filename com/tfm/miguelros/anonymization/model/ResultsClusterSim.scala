package com.tfm.miguelros.anonymization.model

case class ResultsSum(value: Double, total: Double) {
  def +(that: ResultsSum): ResultsSum = {
    ResultsSum(value = this.value + that.value, total = this.total + that.total)
  }

  def ratio: Double = {
    value / total
  }

  def average(totalClusters: Int): Double = ratio / totalClusters.toDouble
}

case class ResultsClusterSim(totalClusters: Int, contacts: ResultsSum, jobRoles: ResultsSum, entities: ResultsSum, disciplines: ResultsSum, industries: ResultsSum) {
  def +(that: ResultsClusterSim): ResultsClusterSim = {
    ResultsClusterSim(
      totalClusters = totalClusters,
      contacts = this.contacts + that.contacts,
      jobRoles = this.jobRoles + that.jobRoles,
      entities = this.entities + that.entities,
      disciplines = this.disciplines + that.disciplines,
      industries = this.industries + that.industries
    )
  }
}
