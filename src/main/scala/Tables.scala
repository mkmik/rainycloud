package it.cnr.aquamaps

/*!
 
 Here we define our data model.
 We have 3 tables: HCAF, HSPEN and the to be generated HSPEC
 
*/

import CassandraConversions._
import org.apache.cassandra.thrift.{ Column, ColumnPath }
import org.apache.log4j.Logger

import stopwatch.Stopwatch
import com.weiglewilczek.slf4s.Logging

//import com.googlecode.avro.marker._

/*!
 
 Tables which have primary key will have to implement this trait.
 */
trait Keyed {
  def key: String
}

case class CellEnvelope(var min: Double, var max: Double, var mean: Double)
object CellEnvelope {
  def apply(): CellEnvelope = CellEnvelope(0, 0, 0)
}

/*!## HCAF
 
 HCAF is the biggest of the three table, but we actually only need to fetch the ocean squares. With the 0.5 deg resolution cells we have:
  
 * hcaf_*:       259'200
 * hcaf ocean:   178'204

 HCAF Table has the csquareCode as key. The companion object contains conversion methods
 */
case class HCAF(var csquareCode: String, var centerLat: Double, var centerLong: Double, var faoAreaM: Int,
  var depth: CellEnvelope,
  var sstAnMean: Double, var sbtAnMean: Double, var salinityMean: Double, var salinityBMean: Double,
  var primProdMean: Double, var iceConAnn: Double, var landDist: Double, var eezFirst: String, var lme: Int) extends Keyed {

  override def toString() = "HCAF(%s)".format(csquareCode)
  def details() = "HSPEC(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(csquareCode, centerLat, centerLong, faoAreaM, depth.min, depth.max, depth.mean, sstAnMean, sbtAnMean, salinityMean, salinityBMean,
                                                           primProdMean, iceConAnn, landDist, eezFirst, lme)
  def key = csquareCode
}

trait ParseHelper {
  def parse(value: Option[String]) = value match {
    case Some("") => -9999.0
    case Some(x) => x.toDouble
    case None => -9999.0
  }

  def parseInt(value: Option[String]) = value match {
    case Some("") => 0
    case Some(x) => x.toInt
    case None => 0
  }

  def parseBool(value: Option[String]) = value match {
    case Some("1") => true
    case Some("0") => false
    case Some("y") => true
    case Some("n") => false
    case Some("") => false
    case None => false
  }
}

object HCAF extends ParseHelper {
  implicit def makeHcaf = HCAF("", 0, 0, 0, CellEnvelope(), 0, 0, 0, 0, 0, 0, 0, "", 0)

  val columns = List("CsquareCode", "OceanArea", "CenterLat", "CenterLong", "FAOAreaM", "DepthMin", "DepthMax", "SSTAnMean", "SBTAnMean", "SalinityMean", "SalinityBMean", "PrimProdMean", "IceConAnn", "LandDist", "EEZFirst", "LME", "DepthMean")

  val condition = "OceanArea > 0"

  def fromTableRow(row: Array[String]): HCAF = build(Map(columns zip row: _*))

  def fromCassandra(x: Iterable[Column]): HCAF = Stopwatch("deserialize") { fromCassandra(columnList2map(x)) }

  def fromCassandra(x: Map[String, Column]): HCAF = build(x mapValues (x => byte2string(x.value.array)))

  def build(x: Map[String, String]) = {
    def get(name: String) = parse(x.get(name))
    def getInt(value: String) = parseInt(x.get(value))
    def getEnvelope(name: String) = CellEnvelope(get(name + "Min"), get(name + "Max"), get(name + "Mean"))
    def faoArea(area: String) = if(area.isEmpty()) -1 else area.toInt

    new HCAF(x.get("CsquareCode").getOrElse(""),
      get("CenterLat"),
      get("CenterLong"),
      faoArea(x.get("FAOAreaM").getOrElse("")),
      getEnvelope("Depth"),
      get("SSTAnMean"),
      get("SBTAnMean"),
      get("SalinityMean"),
      get("SalinityBMean"),
      get("PrimProdMean"),
      get("IceConAnn"),
      get("LandDist"),
      x.get("EEZFirst").getOrElse(""),
      getInt("LME"))
  }
}

case class Envelope(var min: Double, var max: Double, var prefMin: Double, var prefMax: Double)
object Envelope {
  def apply(): Envelope = Envelope(0, 0, 0, 0)
}

/*!## HSPEN

 The HSPEN table describes species and can be loaded in memory:

 * hspen:          9'263

 The HSPEN Table doesn't need a key. The companion object contains conversion methods
 */
case class HSPEN(var speciesId: String, var layer: String, var faoAreas: Set[Int],
  var pelagic: Boolean, var nMostLat: Double, var sMostLat: Double, var wMostLong: Double, var eMostLong: Double,
  var depth: Envelope, var temp: Envelope, var salinity: Envelope, var primProd: Envelope, var landDist: Envelope,
  var meanDepth: Boolean, var iceCon: Envelope, var landDistYN: Boolean) extends Keyed {
  override def toString() = "HSPEN(%s)".format(speciesId)
  def details() = "HSPEN(%s,%s,%s)".format(speciesId, layer, meanDepth)

  def key = speciesId
}

object HSPEN extends ParseHelper with Logging {

  implicit def makeHspen = HSPEN("", "", Set(), false, 0, 0, 0, 0, Envelope(), Envelope(), Envelope(), Envelope(), Envelope(), false, Envelope(), false)

  val columns = List("key", "Layer", "SpeciesID", "FAOAreas", "Pelagic", "NMostLat", "SMostLat", "WMostLong", "EMostLong", "DepthMin", "DepthMax", "DepthPrefMin", "DepthPrefMax", "TempMin", "TempMax", "TempPrefMin", "TempPrefMax", "SalinityMin", "SalinityMax", "SalinityPrefMin", "SalinityPrefMax", "PrimProdMin", "PrimProdMax", "PrimProdPrefMin", "PrimProdPrefMax", "IceConMin", "IceConMax", "IceConPrefMin", "IceConPrefMax", "LandDistMin", "LandDistMax", "LandDistPrefMin", "MeanDepth", "LandDistPrefMax", "LandDistYN")

  def fromTableRow(row: Array[String]): HSPEN = build(Map(columns zip row: _*))

  def fromCassandra(x: Iterable[Column]): HSPEN = fromCassandra(columnList2map(x))

  def fromCassandra(x: Map[String, Column]): HSPEN = build(x mapValues (x => bytebuffer2string(x.value)))

  def build(x: Map[String, String]) = {
    def get(name: String) = parse(x.get(name))
    def getInt(value: String) = parseInt(x.get(value))
    def getBool(name: String) = parseBool(x.get(name))

    def getEnvelope(name: String) = Envelope(get(name + "Min"), get(name + "Max"), get(name + "PrefMin"), get(name + "PrefMax"))

    def faoArea(area: String) = if(area.isEmpty()) -1 else area.toInt
    def layer(layer: String) = if(layer.isEmpty()) " " else layer

    new HSPEN(x.get("SpeciesID").getOrElse("no species"),
      layer(x.get("Layer").getOrElse("")),
      x.get("FAOAreas").getOrElse("").split(",").toList.map { a => faoArea(a.trim) }.toSet,
      getBool("Pelagic"),
      get("NMostLat"),
      get("SMostLat"),
      get("WMostLong"),
      get("EMostLong"),
      getEnvelope("Depth"),
      getEnvelope("Temp"),
      getEnvelope("Salinity"),
      getEnvelope("PrimProd"),
      getEnvelope("LandDist"),
      getBool("MeanDepth"),
      getEnvelope("IceCon"),
                    getBool("LandDistYN"))
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

case class HSPEC(var speciesId: String, var csquareCode: String, var probability: Double, var inBox: Boolean, var inFao: Boolean,
  var faoAreaM: Int, var lme: Int, var eez: String) extends CassandraConfig with CassandraCreator {
  override def keyspaceName = "Aquamaps"
  override def columnFamily = "hspec"

  override def toString() = "HSPEC(%s)".format(key)

  final def toCassandra: Row = Stopwatch("hspecSerialize") {
    (key, List("SpeciesID" --> speciesId,
      "CsquareCode" --> csquareCode))
  }

  final def key = "%s:%s".format(speciesId, csquareCode)
}

object HSPEC extends ParseHelper {
  val columns = List("SpeciesID", "CsquareCode", "Probability", "boundboxYN", "faoareaYN", "FAOAreaM", "LME", "EEZAll")

  /*! These are rarely needed, since we normally don't read back generated HSPEC here */
  def fromTableRow(row: Array[String]): HSPEC = build(Map(columns zip row: _*))

  def build(x: Map[String, String]) = {
    def get(name: String) = parse(x.get(name))
    def getInt(value: String) = parseInt(x.get(value))
    def getBool(name: String) = parseBool(x.get(name))
    def faoArea(area: String) = if(area.isEmpty()) -1 else area.toInt

    new HSPEC(x.get("SpeciesID").getOrElse("no species"),
      x.get("CsquareCode").getOrElse("no csquare code"),
      get("Probability"),
      getBool("boundingboxYN"),
      getBool("faoareaYN"),
      faoArea(x.get("FAOAreaM").getOrElse("")),
      getInt("LME"),
      x.get("EEZAll").getOrElse(""))
  }

}
