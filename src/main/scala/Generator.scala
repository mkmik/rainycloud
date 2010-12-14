package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject
import org.json.simple.JSONArray

import org.apache.cassandra.thrift.{ Column }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import CassandraConversions._


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
	override def columnNames = HCAF.columns

	lazy val hspen = HSPENLoader.load

  def run(task: JSONObject) {
    log.info("hspen task: " + task)

		log.info("hspen size: " + hspen.size)

    val start = task.get("start").asInstanceOf[String]
    val size = task.get("size").asInstanceOf[Long]

    val generated = fetch(start, size).flatMap { case (_, v) => compute(v) }

    store(generated map (_.toCassandra))
  }

	def compute(hcafColumns: Iterable[Column]) : List[HSPEC] = {
		val hcaf = HCAF.fromCassandra(hcafColumns)
		compute(hcaf)
  }

	def compute (hcaf : HCAF) : List[HSPEC]  = {
		List(new HSPEC("a", "b"))
	}
}
