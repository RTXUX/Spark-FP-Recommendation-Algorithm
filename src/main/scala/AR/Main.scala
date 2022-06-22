package AR

import org.apache.spark.{SparkConf, SparkContext}
import AR.config.ARConf

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AR")
    val sc = new SparkContext(conf)
    assert(args.length >= 2, "Usage: AR <input path> <output path>")
    args.foreach(println)
    val arConf = new ARConf(args(0), args(1))

    sc.stop()
  }
}
