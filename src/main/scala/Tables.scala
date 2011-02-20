package it.cnr.aquamaps

/*!
 
 Here we define our data model.
 We have 3 tables: HCAF, HSPEN and the to be generated HSPEC
 
*/

import CassandraConversions._
import org.apache.cassandra.thrift.{ Column, ColumnPath }
import org.apache.log4j.Logger

import stopwatch.Stopwatch

/*!
 
 Tables which have primary key will have to implement this trait.
 */
trait Keyed {
  def key: String
}

/*!## HCAF
 
 HCAF is the biggest of the three table, but we actually only need to fetch the ocean squares. With the 0.5 deg resolution cells we have:
  
 * hcaf_*:       259'200
 * hcaf ocean:   178'204

 HCAF Table has the csquareCode as key. The companion object contains conversion methods
 */
@serializable
class HCAF(val csquareCode: String) extends Keyed {
  override def toString() = "HCAF(%s)".format(csquareCode)

  def key = csquareCode
}

object HCAF {
  val columns = List("CsquareCode", "OceanArea", "CenterLat", "CenterLong", "FAOAreaM", "DepthMin", "DepthMax", "SSTAnMean", "SBTAnMean", "SalinityMean", "SalinityBMean", "PrimProdMean", "IceConAnn", "LandDist", "EEZFirst", "LME", "DepthMean")

  val condition = "OceanArea > 0"

  def fromTableRow(row: Array[String]): HCAF = build(Map(columns zip row: _*))

  def fromCassandra(x: Iterable[Column]): HCAF = Stopwatch("deserialize") { fromCassandra(columnList2map(x)) }

  def fromCassandra(x: Map[String, Column]): HCAF = build(x mapValues (_.value))

  def build(x: Map[String, String]) = {
    new HCAF(x.get("CsquareCode").getOrElse(""))
  }
}

/*!## HSPEN

 The HSPEN table describes species and can be loaded in memory:

 * hspen:          9'263

 The HSPEN Table doesn't need a key. The companion object contains conversion methods
 */
@serializable
class HSPEN(val speciesId: String) extends Keyed {
  override def toString() = "HSPEN(%s)".format(speciesId)
  
  def key = speciesId
}

object HSPEN {
  private val log = Logger.getLogger(this.getClass);

  val columns = List("key", "Layer", "SpeciesID", "FAOAreas", "Pelagic", "NMostLat", "SMostLat", "WMostLong", "EMostLong", "DepthMin", "DepthMax", "DepthPrefMin", "DepthPrefMax", "TempMin", "TempMax", "TempPrefMin", "TempPrefMax", "SalinityMin", "SalinityMax", "SalinityPrefMin", "SalinityPrefMax", "PrimProdMin", "PrimProdMax", "PrimProdPrefMin", "PrimProdPrefMax", "IceConMin", "IceConMax", "IceConPrefMin", "IceConPrefMax", "LandDistMin", "LandDistMax", "LandDistPrefMin", "MeanDepth", "LandDistPrefMax")

  def fromTableRow(row: Array[String]): HSPEN = build(Map(columns zip row: _*))

  def fromCassandra(x: Iterable[Column]): HSPEN = fromCassandra(columnList2map(x))

  def fromCassandra(x: Map[String, Column]): HSPEN = build(x mapValues (_.value))

  def build(x: Map[String, String]) = {
    new HSPEN(x.get("SpeciesID").getOrElse("no species"))
  }

}

/*!## HSPEC

 The HSPEC table is the cartesian product of HSPEN and HCAF and can potentially:

 * total:    1'650'703'652
 
 Fortunately we only care about the non-zero probability out cells, which are currently less:
  
 * output:    56'582'558
  
 The HSPEC Table is declared as a case so that it inherits the Product trait, this way we can easily serialize
 all the fields as CSV
 
 The companion object contains conversion methods.
 */

case class HSPEC(val speciesId: String, val csquareCode: String) extends CassandraConfig with CassandraCreator {
  override def keyspaceName = "Aquamaps"
  override def columnFamily = "hspec"

  override def toString() = "HSPEC(%s)".format(key)

  final def toCassandra: Row = Stopwatch("hspecSerialize") {
    (key, List("SpeciesID" --> speciesId,
      "CsquareCode" --> csquareCode))
  }

  final def key = "%s:%s".format(speciesId, csquareCode)
}

object HSPEC {
  val columns = List("SpeciesID", "CsquareCode", "Probability", "boundboxYN", "faoareaYN", "FAOAreaM", "LME", "EEZAll")

  /*! These are rarely needed, since we normally don't read back generated HSPEC here */
  def fromTableRow(row: Array[String]): HSPEC = build(Map(columns zip row: _*))

  def build(x: Map[String, String]) = {
    new HSPEC(x.get("SpeciesID").getOrElse("no species"),
      x.get("CsquareCode").getOrElse("no csquare code"))
  }

}
