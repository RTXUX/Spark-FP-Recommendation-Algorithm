package AR.config

case class ARConf(
                   var inputFilePath: String = null,
                   var outputFilePath: String = null,
                   var tempFilePath: String = null,
                   var numPartitionA: Int = 0,
                   var numPartitionC: Int = 0
                 )

object ARConf {
  def parseArgs(args: Array[String]): ARConf = {
    require(args.length >= 3, "AR need at least 3 arguments: <input path> <output path> <temp path>")
    val otherArgs = (for(i <- args.indices if i >=3) yield args(i)).toArray
    val arConf = ARConf()
    for (i <- 0 until otherArgs.length / 2) {
      otherArgs(2*i) match {
        case "--numPartitionA" => arConf.numPartitionA = otherArgs(2 * i + 1).toInt
        case "--numPartitionC" => arConf.numPartitionC = otherArgs(2 * i + 1).toInt
      }
    }
    arConf.inputFilePath = args(0)
    arConf.outputFilePath = args(1)
    arConf.tempFilePath = args(2)
    arConf
  }
}