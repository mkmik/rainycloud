package it.cnr.aquamaps

import scala.util.Random

trait HspecAlgorithm {
	def compute (hcaf: HCAF, hspen: Iterable[HSPEN]) : Iterable[HSPEC]
}

class SimpleHSpecAlgorithm extends HspecAlgorithm {
	val random : Random = new Random

	// compute some HSPECs from the given HCAF and all the HSPENs
	// currently just selects some randomly with the same distribution as the actualy computation
	override def compute (hcaf: HCAF, hspen: Iterable[HSPEN]) : Iterable[HSPEC]  = {
		
		hspen.flatMap { pen =>
			if(random.nextInt(30) == 0)
				List(new HSPEC(csquareCode=hcaf.csquareCode, speciesId=pen.speciesId))
			else
				Nil
		}
	}

}
