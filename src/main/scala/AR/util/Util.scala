package AR.util

import AR.entity.AssociationRule
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

  implicit object AssociationRuleOrdering extends Ordering[AssociationRule] {
    override def compare(x: AssociationRule, y: AssociationRule): Int = {
      if (x.confidence == y.confidence) {
        x.consequent compare y.consequent
      } else {
        y.confidence compare x.confidence
      }
    }
  }
}
