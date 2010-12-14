package it.cnr.aquamaps

// hcaf_*:       259'200
// hcaf ocean:   178'204

// hspen:          9'263
// tot:    1'650'703'652
// output:    56'582'558

import CassandraConversions._
import org.apache.cassandra.thrift.{ Column }

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

object HSpen {
  val columns = List("Layer", "SpeciesID", "FAOAreas", "Pelagic", "NMostLat", "SMostLat", "WMostLong", "EMostLong", "DepthMin", "DepthMax", "DepthPrefMin", "DepthPrefMax", "TempMin", "TempMax", "TempPrefMin", "TempPrefMax", "SalinityMin", "SalinityMax", "SalinityPrefMin", "SalinityPrefMax", "PrimProdMin", "PrimProdMax", "PrimProdPrefMin", "PrimProdPrefMax", "IceConMin", "IceConMax", "IceConPrefMin", "IceConPrefMax", "LandDistMin", "LandDistMax", "LandDistPrefMin", "MeanDepth", "LandDistPrefMax")
}
