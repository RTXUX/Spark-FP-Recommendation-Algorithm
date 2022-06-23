package AR.util

import org.apache.spark.SparkContext

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

}
