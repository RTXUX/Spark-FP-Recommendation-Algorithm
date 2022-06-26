package AR


import AR.algorithm.fpm.FPGrowth
import AR.algorithm.rec.UserRecommendation
import org.apache.spark.{SparkConf, SparkContext}
import AR.config.ARConf
import AR.entity.{AssociationRule, FreqItemset}
import AR.util.Util
import AR.util.Util.AssociationRuleOrdering
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

object Main {
  private def initSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("AR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[FreqItemset], classOf[AssociationRule]))
    conf
  }

  def main(args: Array[String]): Unit = {
    val arConf = ARConf.parseArgs(args)
    val conf = Util.adaptiveMemoryStage1(initSparkConf(), arConf)
    val sc = new SparkContext(conf)
    val associationRules = mineFreqPatternAndGenerateAssociationRules(sc, arConf)
    generateUserRecommendations(sc, arConf, associationRules)
    sc.stop()
  }

  def mineFreqPatternAndGenerateAssociationRules(sc: SparkContext, arConf: ARConf): Array[AssociationRule] = {
    val data = sc.textFile(arConf.inputFilePath + "/D.dat", arConf.numPartitionA)
    val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt))
    println(s"Trans: ${transactions.count()}")
    val fp = new FPGrowth(0.092, arConf.numPartitionA)
    // fp.setMinSupport(0.092)
    // fp.setNumPartitions(arConf.numPartitionA)
    val fpModel = fp.run(transactions)
    fpModel.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(fpModel.freqItemsets.count())
    val freqSort = fpModel.freqItemsets.map(i => i.items.mkString(" ")).sortBy(f => f)
    freqSort.saveAsTextFile(arConf.outputFilePath + "/Freq")

    val rules = fpModel.generateAssociationRules(0.0).persist(StorageLevel.MEMORY_AND_DISK)
    // rules.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // println(s"Rules: ${rules.count()}")
    // rules.saveAsObjectFile(arConf.tempFilePath + "/associationRules")
    val sortedRules = rules.sortBy(f => f)(AssociationRuleOrdering, ClassTag[AssociationRule](classOf[AssociationRule])).collect()
    rules.unpersist()
    fpModel.freqItemsets.unpersist()
    sortedRules
  }

  def generateUserRecommendations(sc: SparkContext, arConf: ARConf, associationRules: Array[AssociationRule]): Unit = {
    UserRecommendation.run(sc, arConf, associationRules)
  }
}
