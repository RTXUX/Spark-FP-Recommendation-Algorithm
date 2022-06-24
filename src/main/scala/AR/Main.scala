package AR

import AR.algorithm.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import AR.config.ARConf
import AR.entity.{AssociationRule, FreqItemset}
import AR.util.Util
import AR.util.Util.AssociationRuleOrdering
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {
    val arConf = ARConf.parseArgs(args)

    val conf = new SparkConf().setAppName("AR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[FreqItemset], classOf[AssociationRule]))
    val sc = new SparkContext(conf)
    if (arConf.numPartitionA == 0) {
      arConf.numPartitionA = Util.getTotalCores(sc)
    }
    if (arConf.numPartitionC == 0) {
      arConf.numPartitionC = Util.getTotalCores(sc)
    }
    val associationRules = mineFreqPatternAndGenerateAssociationRules(sc, arConf)
    sc.stop()
  }

  def mineFreqPatternAndGenerateAssociationRules(sc: SparkContext, arConf: ARConf): Array[AssociationRule] = {
    val data = sc.textFile(arConf.inputFilePath + "/D.dat", arConf.numPartitionA)
    val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt))
    println(s"Trans: ${transactions.count()}")
    val fp = new FPGrowth(0.164, arConf.numPartitionA)
    // fp.setMinSupport(0.092)
    // fp.setNumPartitions(arConf.numPartitionA)
    val fpModel = fp.run(transactions)
    fpModel.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(fpModel.freqItemsets.count())
    val freqSort = fpModel.freqItemsets.map(i => i.items.mkString(" ")).sortBy(f => f)
    freqSort.saveAsTextFile(arConf.outputFilePath + "/Freq")

    val rules = fpModel.generateAssociationRules(0.0)
    // rules.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // println(s"Rules: ${rules.count()}")
    // rules.saveAsObjectFile(arConf.tempFilePath + "/associationRules")
    val sortedRules = rules.collect().sorted
    fpModel.freqItemsets.unpersist()
    sortedRules
  }
}
