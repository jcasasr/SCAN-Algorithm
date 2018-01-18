package com.tfm.miguelros.anonymization
package model

case class SimGroup(
  groupDef: GroupDef,
  users: Map[UserID, Int],
  similarities: SimTable
)
