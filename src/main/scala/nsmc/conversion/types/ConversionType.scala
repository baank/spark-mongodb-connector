package nsmc.conversion.types

import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

abstract class ConversionType extends Serializable {
}

case class AtomicType(dt: DataType) extends ConversionType with Serializable {}

case class StructureType(fields: Map[String, ConversionType]) extends ConversionType {
  def sortedFields = {
    fields.toSeq
  }
}

case class SequenceType(elementType: ConversionType) extends ConversionType {

}
