package AR.algorithm.fpm

import AR.entity.{AssociationRule, FreqItemset}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class FPModel(val freqItemsets: RDD[FreqItemset]) extends Serializable {
  def generateAssociationRules(minConfidence: Double): RDD[AssociationRule] = {
    val candidates = freqItemsets.flatMap{ itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty => Some((antecedent.toSeq, (consequent(0), itemset.freq)))
          case _ => None
        }
      }
    }

    candidates.join(freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .filter(f => f._2._1._2.toDouble / f._2._2.toDouble >= minConfidence)
      .map { case(antecedent, ((consequent, freqUnion), freqAntecedent)) =>
        new AssociationRule(antecedent.toArray.sorted, consequent, freqUnion.toDouble / freqAntecedent)
      }
  }
}

class FPGrowth(private var minSupport: Double, private var numPartitions: Int) extends Serializable {
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0, s"Minimal support level must be in range [0,1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions >= 0, s"Number of partitions level must be in range [0,1] but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  private def genFreqItems(data: RDD[Array[Int]], minCount: Long, partitioner: Partitioner): Array[Int] = {
    data.flatMap(t => t).map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  private def genCondTransactions(transaction: Array[Int], itemToRank: Map[Int, Int], partitioner: Partitioner): mutable.ArrayBuffer[Array[Int]] = {
    val res = mutable.ArrayBuffer.empty[Array[Int]]
    val group = mutable.Set.empty[Int]
    val filtered = transaction.flatMap(itemToRank.get)
    java.util.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!group.contains(part)) {
        res.append(filtered.slice(0, i + 1))
        group.add(part)
      }
      i -= 1
    }
    res
  }

  private def genFreqItemsets(
                               data: RDD[Array[Int]],
                               minCount: Long,
                               freqItems: Array[Int],
                               partitioner: Partitioner): RDD[FreqItemset] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    val temp = data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.mapPartitions {iter =>
      val pair = mutable.Map.empty[Array[Int], Long]
      while(iter.hasNext) {
        val arr = iter.next()
        val value = pair.get(arr)
        if (value.isEmpty) {
          pair(arr) = 1L
        } else {
          pair(arr) = value.get + 1L
        }
      }
      pair.iterator
    }.map(tuple => (partitioner.getPartition(tuple._1.last), (tuple._1, tuple._2)))
      .repartitionAndSortWithinPartitions(partitioner).mapPartitions {iter =>
      val coArr = mutable.ArrayBuffer.empty[(Int, (Array[Int], Long))]
      var pair = mutable.Map.empty[Array[Int], Long]
      var pre = partitioner.numPartitions + 1
      while (iter.hasNext) {
        val tuple = iter.next()
        if (pre > partitioner.numPartitions) {
          pre = tuple._1
        } else if (pre != tuple._1) {
          pair.foreach(t => coArr.append((pre, (t._1, t._2))))
          pair = mutable.Map.empty[Array[Int], Long]
          pre = tuple._1
        }

        val arr = tuple._2._1
        val value = pair.get(arr)
        if (value.isEmpty) {
          pair(arr) = tuple._2._2
        } else {
          pair(arr) = tuple._2._2 + value.get
        }
      }
      pair.foreach(t => coArr.append((pre, (t._1, t._2))))
      coArr.iterator
    }.mapPartitions { iter =>
      if (iter.hasNext) {
        val res = mutable.ArrayBuffer.empty[(Int, FPTree[Int])]
        var pre = iter.next()
        var fpTree = new FPTree[Int]()
        fpTree.add(pre._2._1, pre._2._2)
        while(iter.hasNext) {
          val cur = iter.next()
          if (cur._1 == pre._1) {
            fpTree.add(cur._2._1, cur._2._2)
          } else {
            res += ((pre._1, fpTree))
            fpTree = new FPTree[Int]()
            pre = cur
            fpTree.add(pre._2._1, pre._2._2)
          }
        }
        res += ((pre._1, fpTree))
        res.toArray.toIterator
      } else {
        Iterator.empty
      }
    }
    val gen = temp.flatMap{ case(part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }
    gen.map { case(ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

//  private def genFreqItemsets(data: RDD[Array[Int]], minCount: Long, freqItems: Array[Int], partitioner: Partitioner): RDD[FreqItemset] = {
//    val itemToRank = freqItems.zipWithIndex.toMap
//    data.flatMap{ transaction =>
//      genCondTransactions(transaction, itemToRank, partitioner)
//    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
//      (tree, transaction) => tree.add(transaction, 1L),
//      (tree1, tree2) => tree1.merge(tree2)
//    ).flatMap{ case(part, tree) =>
//      tree.extract(minCount, x => partitioner.getPartition(x) == part)
//    }.map { case (ranks, count) =>
//      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
//    }
//  }

  def run(data: RDD[Array[Int]]): FPModel = {
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemset = genFreqItemsets(data, minCount, freqItems, partitioner)
    new FPModel(freqItemset)
  }
}

object FPGrowth {
  class ArrayByValWrapper(val array: Array[Int]) extends Serializable {
    override def hashCode(): Int = java.util.Arrays.hashCode(this.array)

    override def equals(obj: Any): Boolean = {
      obj match {
        case obj: ArrayByValWrapper => java.util.Arrays.equals(this.array, obj.array)
        case obj: Array[Int] => java.util.Arrays.equals(this.array, obj)
        case _ => false
      }
    }
  }
}
