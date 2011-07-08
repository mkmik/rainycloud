package it.cnr.aquamaps

import scala.util.Random
import org.geoscript.geometry._

import scala.collection.mutable.ListBuffer

/*! The core computation of a `HSPEC` row from a pair of `HCAF` and `HSPEN` rows is defined in this trait. */
trait HspecAlgorithm {
  def compute(hcaf: Iterable[HCAF], hspen: HSPEN): Iterable[HSPEC]
}

/*! We are focusing now the scalability aspects of this scenario not on the actualy algorithm, which requires too much
 library dependencies to be able to execute it right now. We know that the correct algorithm generates about 60 million
 records in output, which is about 1/30 of the full cartesian product of the `HSPEC` and `HSPEN` table for 0.5 degree resolution.
 So let's choose randomly. */
class RandomHSpecAlgorithm extends HspecAlgorithm {
  val random: Random = new Random(123)

  override def compute(hcaf: Iterable[HCAF], hspen: HSPEN): Iterable[HSPEC] = {
    hcaf.flatMap { hcaf =>
      if (random.nextInt(30) == 0)
        List(new HSPEC(csquareCode = hcaf.csquareCode, faoAreaM = hcaf.faoAreaM, speciesId = hspen.speciesId, probability = random.nextInt(10000),
          inBox = random.nextBoolean, inFao = random.nextBoolean,
          lme = random.nextInt(10), eez = random.nextInt(10)))
      else
        Nil
    }
  }

}

/*! Or, if you want to generate all 1.8 billion output records (for stress testing for example), use this impl. */
class AllHSpecAlgorithm extends HspecAlgorithm {
  val random: Random = new Random(123)

  // compute all HSPECs from the given HCAF and all the HSPENs
  override def compute(hcaf: Iterable[HCAF], hspen: HSPEN): Iterable[HSPEC] = {
    hcaf.map { hcaf =>
      new HSPEC(csquareCode = hcaf.csquareCode, faoAreaM = hcaf.faoAreaM, speciesId = hspen.speciesId, probability = random.nextInt(10000),
        inBox = random.nextBoolean, inFao = random.nextBoolean,
        lme = random.nextInt(10), eez = random.nextInt(10))
    }
  }

}

/*! This is the current (real) algorithm as ported from the FAO PHP code. */
class CompatHSpecAlgorithm extends HspecAlgorithm {
  val random: Random = new Random(123)


  // compute all HSPECs from the given HCAF and all the HSPENs
  override def compute(hcaf: Iterable[HCAF], hspen: HSPEN): Iterable[HSPEC] = {
    val boundary = rectangle(hspen.nMostLat, hspen.sMostLat, hspen.wMostLong, hspen.eMostLong)
    val faoAreas = hspen.faoAreas

    val res = new ListBuffer[HSPEC]()
    hcaf foreach { hcaf =>
      val inFao = faoAreas contains hcaf.faoAreaM
      //val inFao = random.nextInt(10) < 9
      if (inFao) {
        val inBox = boundary contains Point(hcaf.centerLat, hcaf.centerLong)
        //val inBox = true

        val preparedSeaIce = -9999

        if (inBox) {
          val probability = computeProbability(hcaf, hspen)
          if (probability != 0)
            res += HSPEC(csquareCode = hcaf.csquareCode, faoAreaM = hcaf.faoAreaM, speciesId = hspen.speciesId, probability = probability,
                         inBox = inBox, inFao = inFao, lme = hcaf.lme, eez = hcaf.eezFirst)
        }
      }

    }

    res.toList
  }

  /*! This might seem ugly, but we have to avoid computing useless stuff, we quit as soon as we found a zero value. The computations are also ordered putting
   the ones that defaults frequently first. */
  @inline
  final def computeProbability(hcaf: HCAF, hspen: HSPEN): Double = {
    val landValue = 1.0

    val depthValue = getDepth(hcaf.depth, hspen.pelagic, hspen.depth, hspen.meanDepth)
    if (depthValue == 0)
      return 0

    val sstValue = getSST(hcaf.sstAnMean, hcaf.sbtAnMean, hspen.temp, hspen.layer)
    if (sstValue == 0)
      return 0

    val primaryProductsValue = getPrimaryProduction(hcaf.primProdMean, hspen.primProd)
    if (primaryProductsValue == 0)
      return 0

    val salinityValue = getSalinity(hcaf.salinityMean, hcaf.salinityBMean, hspen.layer, hspen.salinity)
    if (salinityValue == 0)
      return 0

    val seaIceConcentration = 1.0 // TODO: requires data model change for avoiding join
    if (seaIceConcentration == 0)
      return 0

    return landValue * sstValue * depthValue * salinityValue * primaryProductsValue * seaIceConcentration
  }

  @inline
  final def xgetSST(sstAnMean: Double, sbtAnMean: Double, temp: Envelope, layer: String) = 1.0


  @inline
  final def getSST(sstAnMean: Double, sbtAnMean: Double, temp: Envelope, layer: String) = {
    val tempFld = layer match {
      case "s" => sstAnMean
      case "b" => sbtAnMean
      case _ => -9999.0
    }

    if (tempFld == -9999)
      1.0
    else if (tempFld < temp.min)
      0.0
    else if (tempFld >= temp.min && tempFld < temp.prefMin)
      (tempFld - temp.min) / (temp.prefMin - temp.min)
    else if (tempFld >= temp.prefMin && tempFld <= temp.prefMax)
      1.0
    else if (tempFld > temp.prefMax && tempFld <= temp.max)
      (temp.max - tempFld) / (temp.max - temp.prefMax)
    else
      0.0
  }

  @inline
  final def xgetDepth(_hcafDepth: CellEnvelope, pelagic: Boolean, hspenDepth: Envelope, hspenMeanDepth: Boolean) = 1.0

  @inline
  final def getDepth(_hcafDepth: CellEnvelope, pelagic: Boolean, hspenDepth: Envelope, hspenMeanDepth: Boolean) = {
    val hcafDepth = if (hspenMeanDepth)
      CellEnvelope(_hcafDepth.mean, _hcafDepth.mean, _hcafDepth.mean)
    else
      CellEnvelope(_hcafDepth.min, _hcafDepth.max, _hcafDepth.mean)

    // Check on hspenMeanDepth added from HSPEC version 2 (used from release 1.7)
    if (hspenDepth.min == -9999)
      1.0
    else if (hcafDepth.max == -9999)
      1.0
    else if (hcafDepth.max < hspenDepth.min)
      0.0
    else if ((hcafDepth.max < hspenDepth.prefMin) && (hcafDepth.max >= hspenDepth.min))
      (hcafDepth.max - hspenDepth.min) / (hspenDepth.prefMin - hspenDepth.min)
    else if (pelagic)
      1.0
    else if (hspenDepth.prefMax != -9999) {
      if (hcafDepth.max >= hspenDepth.prefMin && hcafDepth.min <= hspenDepth.prefMax)
        1.0
      else if (hcafDepth.min >= hspenDepth.prefMax) {
        if ((hcafDepth.max.intValue()) - hspenDepth.prefMax.intValue() != 0) {
          val tempdepth = (hspenDepth.max - hcafDepth.min) / (hspenDepth.max.toInt - hspenDepth.prefMax.toInt)
          if (tempdepth < 0) 0.0 else tempdepth
        } else 0.0
      } else 0.0
    } else 0.0
  }

  @inline
  final def xgetSalinity(hcafSalinitySMean: Double, hcafSalinityBMean: Double, layer: String, hspenSalinity: Envelope) = 1.0


  @inline
  final def getSalinity(hcafSalinitySMean: Double, hcafSalinityBMean: Double, layer: String, hspenSalinity: Envelope) = {
    val smean = layer match {
      case "s" => hcafSalinitySMean
      case "b" => hcafSalinityBMean
      case _ => -9999
    }

    if (smean == -9999 || hspenSalinity.min == -9999)
      1.0
    else if (smean < hspenSalinity.min)
      0.0
    else if (smean >= hspenSalinity.min && smean < hspenSalinity.prefMin)
      (smean - hspenSalinity.min) / (hspenSalinity.prefMin - hspenSalinity.min)
    else if (smean >= hspenSalinity.prefMin && smean <= hspenSalinity.prefMax)
      1.0
    else if (smean > hspenSalinity.prefMax && smean <= hspenSalinity.max)
      (hspenSalinity.max - smean) / (hspenSalinity.max - hspenSalinity.prefMax)
    else
      0.0
  }

  @inline
  final def xgetPrimaryProduction(hcafPrimProdMean: Double, hspenPrimProd: Envelope) = 1.0


  @inline
  final def getPrimaryProduction(hcafPrimProdMean: Double, hspenPrimProd: Envelope) = {
    if (hcafPrimProdMean == 0)
      1.0
    else if (hcafPrimProdMean < hspenPrimProd.min)
      0.0
    else if ((hcafPrimProdMean >= hspenPrimProd.min) && (hcafPrimProdMean < hspenPrimProd.prefMin))
      (hcafPrimProdMean - hspenPrimProd.min) / (hspenPrimProd.prefMin - hspenPrimProd.min)
    else if ((hcafPrimProdMean >= hspenPrimProd.prefMin) && (hcafPrimProdMean <= hspenPrimProd.prefMax))
      1.0
    else if ((hcafPrimProdMean > hspenPrimProd.prefMax) && (hcafPrimProdMean <= hspenPrimProd.max))
      (hspenPrimProd.max - hcafPrimProdMean) / (hspenPrimProd.max - hspenPrimProd.prefMax)
    else
      0.0
  }

  def rectangle(n: Double, s: Double, w: Double, e: Double) = Polygon(LineString(Point(n, w), Point(n, e), Point(s, e), Point(s, w), Point(n, w)), Nil)

}
