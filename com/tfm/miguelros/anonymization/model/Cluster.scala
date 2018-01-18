package com.tfm.miguelros.anonymization
package model

case class Cluster(clusterId: ClusterID, users: Set[AssignedUser])
