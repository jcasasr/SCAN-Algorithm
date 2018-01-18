package com.tfm.miguelros.anonymization

case class Arguments(clusteringOut: String, similarities: String, k: Int, executions: Int)

object Arguments {
  def processArgs(args: Array[String]): Arguments = {
    if (args.length != 4) {
      println("xxx wrong. syntax: CLUSTERING_OUT SIMILARITIES K EXECUTIONS ")
      println("xxx " + args.length + " -- " + args)
      sys.exit(1)
    }
    Arguments(args(0), args(1), args(2).toInt, args(3).toInt)
  }
}