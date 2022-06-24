package AR.entity

class FreqItemset(val items: Array[Int], val freq: Long) extends Serializable {
  override def toString: String = {
    s"${items.mkString("{", ",", "}")}: ${freq}"
  }
}
