package AR.algorithm.fpm

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FPTree[T] extends Serializable {
  import FPTree._

  val root: Node[T] = new Node(null)

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      var summary = summaries.getOrElse(item, null)
      if (summary == null) {
        summary = new Summary[T]
        summaries(item) = summary
      }
      summary.count += count
      var child = curr.children.getOrElse(item, null)
      if (child == null) {
        val newNode = new Node[T](curr)
        newNode.item = item
        summary.nodes += newNode
        child = newNode
        curr.children(item) = newNode
      }
      child.count += count
      curr = child
    }
    this
  }

  def merge(other: FPTree[T]): FPTree[T] = {
    other.transactions.foreach{ case(t, c) =>
      add(t, c)
    }
    this
  }

  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case(item, child) =>
      getTransactions(child).map { case(t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  private def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    val summary = summaries.getOrElse(suffix, null)
    if (summary == null) return tree
    summary.nodes.foreach { node =>
      var t = List.empty[T]
      var curr = node.parent
      while(!curr.isRoot) {
        t = curr.item :: t
        curr = curr.parent
      }
      tree.add(t, node.count)
    }
    tree
  }

  def extract(minCount: Long, validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] =  {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single(item :: Nil, summary.count) ++
          project(item).extract(minCount).map { case(t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

object FPTree {
  private[fpm] class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    var children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

  private[fpm] class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }
}
