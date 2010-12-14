package it.cnr.aquamaps

// hcaf_*:       259'200
// hcaf ocean:   178'204

// hspen:          9'263
// tot:    1'650'703'652
// output:    56'582'558

import CassandraConversions._
import org.apache.cassandra.thrift.{ Column, ColumnPath }
import org.apache.log4j.Logger

class HCAF (val csquareCode : String) {
	override def toString() = "HCAF(%s)".format(csquareCode)
}


object HCAF {
  val columns = List("CsquareCode", "OceanArea", "CenterLat", "CenterLong", "FAOAreaM", "DepthMin", "DepthMax", "SSTAnMean", "SBTAnMean", "SalinityMean", "SalinityBMean", "PrimProdMean", "IceConAnn", "LandDist", "EEZFirst", "LME", "DepthMean")

	val condition = "OceanArea > 0"

	def fromCassandra(x : Iterable[Column]) : HCAF = fromCassandra(columnList2map(x))

	def fromCassandra(x : Map[String, Column]) : HCAF = build(x mapValues (_.value))

	def build(x : Map[String, String]) = {
		new HCAF(x.get("CsquareCode").getOrElse(""))
	}
}

class HSPEN(val speciesId : String) {
	override def toString() = "HSPEN(%s)".format(speciesId)
}

object HSPEN {
  private val log = Logger.getLogger(this.getClass);


  val columns = List("Layer", "SpeciesID", "FAOAreas", "Pelagic", "NMostLat", "SMostLat", "WMostLong", "EMostLong", "DepthMin", "DepthMax", "DepthPrefMin", "DepthPrefMax", "TempMin", "TempMax", "TempPrefMin", "TempPrefMax", "SalinityMin", "SalinityMax", "SalinityPrefMin", "SalinityPrefMax", "PrimProdMin", "PrimProdMax", "PrimProdPrefMin", "PrimProdPrefMax", "IceConMin", "IceConMax", "IceConPrefMin", "IceConPrefMax", "LandDistMin", "LandDistMax", "LandDistPrefMin", "MeanDepth", "LandDistPrefMax")

	def fromCassandra(x : Iterable[Column]) : HSPEN = fromCassandra(columnList2map(x))

	def fromCassandra(x : Map[String, Column]) : HSPEN = build(x mapValues (_.value))

	def build(x : Map[String, String]) = {
//		println(x.keys)
		new HSPEN(x.get("SpeciesID").getOrElse("no species"))
	}

}


class HSPEC (val speciesId : String, val csquareCode : String) extends CassandraConfig with CassandraCreator {
  override def keyspaceName = "Aquamaps"
	override def columnFamily = "hspec"

	def toCassandra : Row = ("0", List("SpeciesID" --> speciesId,
																									"CsquareCode" --> csquareCode))
}

object HSPEC {
	val columns = List("SpeciesID", "CsquareCode", "Probability", "boundboxYN", "faoareaYN", "FAOAreaM", "LME", "EEZAll")
}
