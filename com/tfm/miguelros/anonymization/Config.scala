package com.tfm.miguelros.anonymization

object Config {
  final val HDFS_MODEL_PATH = "/user/miguel.ros/anonymization"
  final val HDFS_CACHE_SIMILARITIES = HDFS_MODEL_PATH + "/similarities"
  final val HDFS_CACHE_CLUSTERIZATION = HDFS_MODEL_PATH + "/clusterization"

  final val CACHE_MINI_DEMO = false
  if (CACHE_MINI_DEMO) {
    println("xxx MINI DEMO true")
  } else {
    println("xxx MINI DEMO false")
  }

  final var CACHE_PARTITIONS   = 200
  final var CLUSTER_PARTITIONS = 293
  final var EVAL_PARTITIONS    = 50

  if (CACHE_MINI_DEMO){
    CACHE_PARTITIONS   = 50
    CLUSTER_PARTITIONS = 50
    EVAL_PARTITIONS    = 20
  }

  final val PARALLEL_TASKS  = 10
  final val NUM_TOP_RAND    = 30
}