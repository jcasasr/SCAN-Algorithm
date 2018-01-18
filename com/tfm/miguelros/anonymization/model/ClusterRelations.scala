package com.tfm.miguelros.anonymization
package model

case class ClusterRelations(clusterID: ClusterID, relations: Map[ClusterID, Int])
