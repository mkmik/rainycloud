package it.cnr.aquamaps

import scala.util.Random
import org.geoscript.geometry._

import scala.collection.mutable.ListBuffer

import org.apache.log4j.Logger

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
      if (random.nextInt(12) == 0)
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
  private val log = Logger.getLogger(this.getClass);

  val random: Random = new Random(123)


  // compute all HSPECs from the given HCAF and all the HSPENs
  override def compute(hcaf: Iterable[HCAF], hspen: HSPEN): Iterable[HSPEC] = {
    val boundary = rectangle(hspen.nMostLat, hspen.sMostLat, hspen.wMostLong, hspen.eMostLong)
    val faoAreas = hspen.faoAreas

    val res = new ListBuffer[HSPEC]()
    hcaf foreach { hcaf =>
      //val inFao = faoAreas contains hcaf.faoAreaM
      //val inFao = random.nextInt(10) < 9
      val inFao= true
      if (inFao) {
        //val inBox = boundary contains Point(hcaf.centerLat, hcaf.centerLong)
        val inBox = true

        val preparedSeaIce = -9999

        if (inBox) {
          val probability = computeProbability(hcaf, hspen)
          if (probability != 0)
            res += HSPEC(csquareCode = hcaf.csquareCode, faoAreaM = hcaf.faoAreaM, speciesId = hspen.speciesId, probability = probability,
                         inBox = inBox, inFao = inFao, lme = hcaf.lme, eez = hcaf.eezFirst)
        } else {
          log.info("Ignored: not in box")
        }
      } else {
          log.info("Ignored: not in fao: hspec(%s) vs hcaf(%s)".format(faoAreas, hcaf.faoAreaM))
      }

    }

    res.toList
  }

  /*! This might seem ugly, but we have to avoid computing useless stuff, we quit as soon as we found a zero value. The computations are also ordered putting
   the ones that defaults frequently first. */
  @inline
  final def computeProbability(hcaf: HCAF, hspen: HSPEN): Double = {
    //log.info("computing prob for %s and %s".format(hcaf.details, hspen.details));

    if(!checkHSpen(hspen))
      return 0

    val depthValue = getDepth(hcaf.depth, hspen.pelagic, hspen.depth, hspen.meanDepth)
    val newDepthValue = translatedGetDepth(hcaf.depth, hspen.pelagic, hspen.depth, hspen.meanDepth)
    if(depthValue != newDepthValue)
      log.info("depth value: old: %s new: %s".format(depthValue, newDepthValue))
    if (depthValue == 0)
      return 0

    val sstValue = getSST(hcaf.sstAnMean, hcaf.sbtAnMean, hspen.temp, hspen.layer)
    // log.info("sst value %s".format(sstValue))
    if (sstValue == 0)
      return 0

    val primaryProductsValue = getPrimaryProduction(hcaf.primProdMean, hspen.primProd)
    // log.info("prim prod value %s".format(primaryProductsValue))
    if (primaryProductsValue == 0)
      return 0

    val salinityValue = getSalinity(hcaf.salinityMean, hcaf.salinityBMean, hspen.layer, hspen.salinity)
    // log.info("salinity value %s".format(salinityValue))
    if (salinityValue == 0)
      return 0

    val landDistValue = getLandDist(hcaf.landDist, hspen.landDist, hspen.landDistYN)
    // log.info("land dist value %s".format(landDistValue))
    if (landDistValue == 0)
      return 0


    val seaIceConcentration = getIceConcentration(hcaf.iceConAnn, hspen.iceCon)
    // log.info("sea ice concentration %s".format(seaIceConcentration))
    if (seaIceConcentration == 0)
      return 0

    val prob = sstValue * depthValue * salinityValue * primaryProductsValue * landDistValue * seaIceConcentration
    //log.info("computed prob %s".format(prob))
    if(prob < 0.01) {
      //log.info("trimming prob %s".format(prob))
      0
    } else {
      //log.info(" -- outputed with prob %s (%s, %s)".format(prob, hcaf, hspen))
      prob
    }
  }

  @inline
  final def checkHSpen(hspen: HSPEN) = {
    //    log.info("checking hspen %s   -> landDist.max %s".format(hspen.details, hspen.landDist.max))
    checkEnvelope(hspen.iceCon) // && checkEnvelope(hspen.landDist)
  }

  @inline
  final def checkEnvelope(env: Envelope) = {
    env.min != -9999 && env.max != -9999 && env.prefMin != -9999 && env.prefMax != -9999
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

  // this is obsolete, is here only to check if we have some deviations from the php algo
  @inline
  final def translatedGetDepth(_hcafDepth: CellEnvelope, pelagic: Boolean, hspenDepth: Envelope, hspenMeanDepth: Boolean) = {

    if(hspenDepth.min == -9999) {
      1
    } else {
      val hcafDepth = if (hspenMeanDepth)
                        CellEnvelope(_hcafDepth.mean, _hcafDepth.mean, _hcafDepth.mean)
                      else
                        CellEnvelope(_hcafDepth.min, _hcafDepth.max, _hcafDepth.mean)

      if (hcafDepth.max == -9999)
        1
      else {
        if (hcafDepth.max < hspenDepth.min)
          0
        else {
          if (hcafDepth.max < hspenDepth.prefMin && hcafDepth.max >= hspenDepth.min) {
            (hcafDepth.max - hspenDepth.min) / (hspenDepth.prefMin - hspenDepth.min)
          }
          else {
            if (pelagic && !hspenMeanDepth)
              1
            else {
              if (hcafDepth.max >= hspenDepth.prefMin && hcafDepth.min <= hspenDepth.prefMax)
                1
              else {
                if (hspenDepth.prefMax != -9999) {
                  if (hcafDepth.min >= hspenDepth.prefMax) {
                    var resDepth = -1.0;

                    //to correct div by zero
                    if ((hspenDepth.max - hspenDepth.prefMax) != 0) {
                      resDepth = (hspenDepth.max - hcafDepth.min) / (hspenDepth.max - hspenDepth.prefMax)
                    } else 
                      resDepth = 0
                    
                    // ATTENTION
                    if ((hspenDepth.max - hspenDepth.prefMax) != 0)
                      resDepth = (hspenDepth.max - hcafDepth.min) / (hspenDepth.max - hspenDepth.prefMax)
                    else 
                      resDepth = 0
                    
                    if(resDepth < 0)
                      resDepth = 0
                    
                    resDepth
                  } else {
                    0
                  }
                } else {
                  0
                }
              }
            }
          }
        }
      }
    }
  }

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
    else if (pelagic && !hspenMeanDepth)
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

  @inline
//  final def getLandDist(hspen: HSPEN, hcaf: HCAF) = {
  final def getLandDist(hcafLandDist: Double, hspenLandDist: Envelope, landDistYN: Boolean) = {

    if (!landDistYN)
      1
    else if (hcafLandDist == -9999) {
      1
    } else if (hcafLandDist < hspenLandDist.min) {
      0
    } else if (hcafLandDist >= hspenLandDist.min && hcafLandDist < hspenLandDist.prefMin) {
      (hcafLandDist - hspenLandDist.min) / (hspenLandDist.prefMin - hspenLandDist.min)
    } else if (hspenLandDist.prefMax > 1000) {
      1
    } else if (hcafLandDist >= hspenLandDist.prefMin && hcafLandDist < hspenLandDist.prefMax) {
      1
    } else if (hcafLandDist >= hspenLandDist.prefMax && hcafLandDist < hspenLandDist.max) {
      (hspenLandDist.max - hcafLandDist) / (hspenLandDist.max - hspenLandDist.prefMax)
    } else {
      0
    }

  }


  @inline
  final def getIceConcentration(iceConAnn: Double, iceCon: Envelope) = {
//    log.info("iceConAnn: %s, iceCon: %s".format(iceConAnn, iceCon))

    if (iceCon.min == -9999)
      1.0
    else {
      if (iceConAnn == -9999) 
        0.0
      else {
        if (iceConAnn < iceCon.min)
          0
        else if (iceConAnn >= iceCon.min && iceConAnn < iceCon.prefMin) // redundant 'and' lhs
          (iceConAnn - iceCon.min) / (iceCon.prefMin - iceCon.min)
        else if (iceConAnn >= iceCon.prefMin && iceConAnn <= iceCon.prefMax)
          1
        else if (iceConAnn > iceCon.prefMax && iceConAnn <= iceCon.max)
          (iceCon.max - iceConAnn) / (iceCon.max - iceCon.prefMax)
        else if (iceConAnn > iceCon.max) // redundant fall through
          0
        else 
          0
      }
    }
  }

  def rectangle(n: Double, s: Double, w: Double, e: Double) = Polygon(LineString(Point(n, w), Point(n, e), Point(s, e), Point(s, w), Point(n, w)), Nil)

}
