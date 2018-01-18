package com.tfm.miguelros.anonymization
package model

case class User(
  userId: UserID,
  jobRoles: Set[Int],
  careerLevel: Int,
  disciplineId: Int,
  industryId: Int,
  region: Int,
  yearsExperience: Int,
  entities: Set[Int],
  relations: Set[UserID]
)
