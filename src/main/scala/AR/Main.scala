package AR

import org.apache.spark.{SparkConf, SparkContext}
import AR.config.ARConf
import AR.entity.FreqItemset
import AR.util.Util
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {
    val arConf = ARConf.parseArgs(args)

    val conf = new SparkConf().setAppName("AR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[FreqItemset]))
    val sc = new SparkContext(conf)
    if (arConf.numPartitionA == 0) {
      arConf.numPartitionA = Util.getTotalCores(sc)
    }
    if (arConf.numPartitionC == 0) {
      arConf.numPartitionC = Util.getTotalCores(sc)
    }
    mineFreqPattern(sc, arConf)
    sc.stop()
  }

  def mineFreqPattern(sc: SparkContext, arConf: ARConf): Unit = {
    val data = sc.textFile(arConf.inputFilePath + "/D.dat", arConf.numPartitionA)
    val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt))
    println(s"Trans: ${transactions.count()}")
    val fp = new FPGrowth()
    fp.setMinSupport(0.164)
    val fpModel = fp.run(transactions)
    fpModel.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK)
    println(fpModel.freqItemsets.count())
    val freqSort = fpModel.freqItemsets.map(i => i.items.mkString(" ")).sortBy(f=>f)
    freqSort.saveAsTextFile(arConf.outputFilePath + "/Freq")
  }
}
