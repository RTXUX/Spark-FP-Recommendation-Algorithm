package AR.algorithm.rec

import AR.config.ARConf
import org.apache.spark.SparkContext
import AR.entity.AssociationRule
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object UserRecommendation {

  object SizeOrdering extends Ordering[(HashSet[Int], Long)] {
    override def compare(x: (HashSet[Int], Long), y: (HashSet[Int], Long)): Int = {
      x._1.size compare y._1.size
    }
  }

  def run(sc: SparkContext, arConf: ARConf, assRule: Array[AssociationRule]): Unit = {
    val userData = sc.textFile(arConf.inputFilePath + "/U.dat", arConf.numPartitionC)
      .map(i => i.trim.split(' ').map(f => f.toInt).sorted).map(i => HashSet(i: _*)).zipWithIndex()

    val assRulesBroadcast = sc.broadcast(assRule)

    def doUserRec(iter: Iterator[(HashSet[Int], Long)]): Iterator[(Long, Int)] = {
      val res = mutable.ArrayBuffer.empty[(Long, Int)]
      val assRuleValue = assRulesBroadcast.value
      while (iter.hasNext) {
        val user = iter.next
        val userProf = user._1
        var rec = 0
        breakable {
          for (i <- assRuleValue.indices) {
            var flag = true
            val rule = assRuleValue(i)
            val consequent = rule.consequent
            if (userProf.size >= rule.antecedent.length && !userProf.contains(consequent)) {
              breakable {
                for (antecedent <- rule.antecedent) {
                  if (!userProf.contains(antecedent)) {
                    flag = false
                    break()
                  }
                }
              }
              if (flag) {
                rec = consequent
                break()
              }
            }
          }
        }
        res.append((user._2, rec))
      }
      res.iterator
    }

    val splitPoint = Array(25, 50, 100, 500)
    val splitRDDs = Array.ofDim[RDD[(Long, Int)]](splitPoint.length + 1)
    for (i <- splitRDDs.indices) {
      val filteredRDD = if (i == 0) {
        userData.filter(_._1.size <= splitPoint(0))
      } else if (i == splitRDDs.length - 1) {
        userData.filter(_._1.size > splitPoint(i - 1))
      } else {
        userData.filter(r => r._1.size > splitPoint(i - 1) && r._1.size <= splitPoint(i))
      }
      splitRDDs(i) = filteredRDD.repartition(arConf.numPartitionC)(SizeOrdering).mapPartitions(doUserRec).persist(StorageLevel.MEMORY_AND_DISK)
    }

    splitRDDs.reduce(_ ++ _).sortByKey().map(_._2).saveAsTextFile(arConf.outputFilePath + "/Rec")
    // userData.repartition(arConf.numPartitionC)(SizeOrdering).mapPartitions(doUserRec).persist(StorageLevel.MEMORY_AND_DISK).sortByKey().map(_._2).saveAsTextFile(arConf.outputFilePath + "/Rec")
    assRulesBroadcast.destroy()
  }
}
