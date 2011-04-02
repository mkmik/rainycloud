package it.cnr.aquamaps

import scala.util.Random

/*! The core computation of a `HSPEC` row from a pair of `HCAF` and `HSPEN` rows is defined in this trait. */
trait HspecAlgorithm {
  def compute(hcaf: HCAF, hspen: Iterable[HSPEN]): Iterable[HSPEC]
}

/*! We are focusing now the scalability aspects of this scenario not on the actualy algorithm, which requires too much
 library dependencies to be able to execute it right now. We know that the correct algorithm generates about 60 million
 records in output, which is about 1/30 of the full cartesian product of the `HSPEC` and `HSPEN` table for 0.5 degree resolution.
 So let's choose randomly. */
class RandomHSpecAlgorithm extends HspecAlgorithm {
  val random: Random = new Random(123)

  override def compute(hcaf: HCAF, hspen: Iterable[HSPEN]): Iterable[HSPEC] = {

    hspen.flatMap { pen =>
      if (random.nextInt(30) == 0)
        List(new HSPEC(csquareCode = hcaf.csquareCode, speciesId = pen.speciesId))
      else
        Nil
    }
  }

}

/*! Or, if you want to generate all 1.8 billion output records (for stress testing for example), use this impl. */
class AllHSpecAlgorithm extends HspecAlgorithm {
  val random: Random = new Random

  // compute all HSPECs from the given HCAF and all the HSPENs
  override def compute(hcaf: HCAF, hspen: Iterable[HSPEN]): Iterable[HSPEC] = {
    hspen.map { pen => new HSPEC(csquareCode = hcaf.csquareCode, speciesId = pen.speciesId)}
  }

}
