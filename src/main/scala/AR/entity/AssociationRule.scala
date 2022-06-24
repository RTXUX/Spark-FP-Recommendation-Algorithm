package AR.entity

class AssociationRule(val antecedent: Array[Int], val consequent: Int, val confidence: Double) extends Serializable {
  override def toString: String = {
    s"${antecedent.mkString("{", ", ", "}")} => ${consequent}: ${confidence}"
  }
}
