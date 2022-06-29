package AR.util

import AR.config.ARConf
import AR.entity.AssociationRule
import org.apache.spark.{SparkConf, SparkContext}

object Util {

  def currentActiveExecutors(sc: SparkContext): Int = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList.length
  }

  def getTotalCores(sc: SparkContext): Int = {
    val numExecutors = currentActiveExecutors(sc)
    val numCoresPerExecutor = sc.getConf.getInt("spark.executor.cores", 1)
    numExecutors * numCoresPerExecutor
  }

  implicit object AssociationRuleOrdering extends Ordering[AssociationRule] {
    override def compare(x: AssociationRule, y: AssociationRule): Int = {
      if (x.confidence == y.confidence) {
        x.consequent compare y.consequent
      } else {
        y.confidence compare x.confidence
      }
    }
  }

  def adaptiveMemoryStage1(conf: SparkConf, arConf: ARConf): SparkConf = {
    val execCores = conf.getInt("spark.executor.cores", 24)
    val memoryAllocated = conf.getSizeAsGb("spark.executor.memory", "30G")
    val executors = conf.getInt("spark.executor.instances", 1)
    val adaptiveCores = if (!arConf.smallset){
      Math.max(Math.min(execCores, memoryAllocated / 8).toInt, 1)
    } else {
      println("Running on small dataset, not using adaptive strategy")
      Math.max(execCores, 1)
    }
    val partitions = adaptiveCores * executors * 16
    arConf.numPartitionA = partitions
    arConf.numPartitionC = partitions / 16
    val res = conf.clone()
    res.set("spark.driver.allowMultipleContexts", "true")
    res.set("spark.executor.cores", adaptiveCores.toString)
    res
  }

  // Unused, spark deployed in YARN in cluster mode doesn't support multiple contexts
  def adaptiveMemoryStage2(conf: SparkConf, arConf: ARConf): SparkConf = {
    val execCores = conf.getInt("spark.executor.cores", 24)
    val memoryAllocated = conf.getSizeAsGb("spark.executor.memory", "30G")
    val executors = conf.getInt("spark.executor.instances", 1)
    val adaptiveCores = Math.max(Math.min(execCores, memoryAllocated / 10 * 4).toInt, 1)
    val partitions = adaptiveCores * executors * 2
    arConf.numPartitionC = partitions
    val res = conf.clone()
    res.set("spark.driver.allowMultipleContexts", "true")
    res.set("spark.executor.cores", adaptiveCores.toString)
    res.set("spark.default.parallelism", (partitions * 4).toString)
    res
  }
}
