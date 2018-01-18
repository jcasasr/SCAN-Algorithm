package com.tfm.miguelros.anonymization
package model

case class ClusterFull(id: ClusterID, groupDef: GroupDef, users: Map[UserID, User])
