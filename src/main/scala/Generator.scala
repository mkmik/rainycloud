package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject
import org.json.simple.JSONArray

import org.apache.cassandra.thrift.{ Column }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import CassandraConversions._

/*
object HSPENLoader extends CassandraFetcher {
	override def keyspaceName = "Aquamaps"
	override def columnFamily = "hspen"
	
}
*/

object HSPECGenerator extends Worker with CassandraFetcher with CassandraSink with Mutable {
  import scala.collection.immutable.HashMap

  private val log = Logger.getLogger(this.getClass);

  override def keyspaceName = "Aquamaps"
	override def columnFamily = "hcaf"


  def run(task: JSONObject) {
    log.info("hspen task: " + task)

    val start = task.get("start").asInstanceOf[String]
    val size = task.get("size").asInstanceOf[Long]

    val generated = fetch(start, size).flatMap { case (_, v) => compute(v) }

    store(generated)
  }

	def compute(hcafColumns: Iterable[Column]): Iterable[Column] = {
		val hcaf = HCAF.fromCassandra(hcafColumns)
    //log.info("computing hcaf row " + hcaf )
    List()
  }
}
