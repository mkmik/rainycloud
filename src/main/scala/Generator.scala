package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject
import org.json.simple.JSONArray

import org.apache.cassandra.thrift.{ Column }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random

import CassandraConversions._


import stopwatch.Stopwatch
import stopwatch.StopwatchGroup
import stopwatch.StopwatchRange
import stopwatch.TimeUnit._


object HSPENLoader extends CassandraFetcher {
  private val log = Logger.getLogger(this.getClass);

	override def keyspaceName = "Aquamaps"
	override def columnFamily = "hspen"
	override def columnNames = HSPEN.columns

	def load = {
		val hspen = fetch("", 10000)
		log.info("hspen loaded: " + hspen.size)

//		val h : String = hspen
		
		hspen.map {case (_, v) => HSPEN.fromCassandra(v) }
	}
}


object HSPECGenerator extends Worker with CassandraFetcher with CassandraSink  {
  private val log = Logger.getLogger(this.getClass);

  override def keyspaceName = "Aquamaps"
	override def columnFamily = "hcaf"
	override def outputColumnFamily = "hspec"
	override def columnNames = HCAF.columns

	val random : Random = new Random

	lazy val hspen = HSPENLoader.load
	
	//Watch.run

	Stopwatch.enabled = false
	Stopwatch.range = StopwatchRange(0 seconds, 15 seconds, 500 millis)

  def run(task: JSONObject) {
    val start = task.get("start").asInstanceOf[String]
    val size  = task.get("size" ).asInstanceOf[Long]

		
			val generated = (Stopwatch("fetch") {fetch(start, size)}).flatMap { case (_, v) => compute(v) }
		

    store((Stopwatch("serialize") {generated map (_.toCassandra)}))
  }

	def compute(hcafColumns: Iterable[Column]) : Iterable[HSPEC] = {
		val hcaf = HCAF.fromCassandra(hcafColumns)
		Stopwatch("compute") {
			compute(hcaf);
		}
  }

	// compute some HSPECs from the given HCAF and all the HSPENs
	// currently just selects some randomly with the same distribution as the actualy computation
	def compute (hcaf : HCAF) : Iterable[HSPEC]  = {
		hspen.flatMap { pen =>
			if(random.nextInt(30) == 0)
				List(new HSPEC(csquareCode=hcaf.csquareCode, speciesId=hspen.head.speciesId))
			else
				Nil
		}
	}
}
