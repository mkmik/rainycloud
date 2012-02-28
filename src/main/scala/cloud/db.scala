package it.cnr.aquamaps.cloud

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Dispatchers
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._
import java.util.concurrent._
import com.typesafe.config.ConfigFactory


import com.google.inject._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }
import uk.me.lings.scalaguice.InjectorExtensions._

import com.weiglewilczek.slf4s.Logging
import it.cnr.aquamaps.jdbc.LiteDataSource
import resource._
import java.io._

import it.cnr.aquamaps._

import java.sql.{Connection, ResultSet}


object DBAccessor {
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
}

trait DBAccessor {
  import DBAccessor._

  import java.sql.DriverManager
  import org.postgresql.PGConnection
  Class.forName("org.postgresql.Driver")

  def connection(table: Table) = {
    val urlComps = table.jdbcUrl.split(";")
    val cleanUrl = urlComps(0)
    val user = urlComps(1).split("=")(1)
    val password = urlComps(2).split("=")(1)

    DriverManager.getConnection(cleanUrl, user, password)
  }

  def getCopyApi(con: Connection) = con.asInstanceOf[PGConnection].getCopyAPI()

  def execute(sql: String)(implicit con: Connection) {
    for(st <- managed(con.createStatement))
      st.execute(sql)
  }

  def query[A](sql: String)(body: ResultSet => A)(implicit con: Connection): Option[A] = {
    (for(st <- managed(con.createStatement))
       yield body(st.executeQuery(sql))).opt
  }

}

class DBTableReader[A](val table: Table, val query: Option[String] = None) extends TableReader[A] with DBAccessor {
  import DBAccessor._

  def reader = {
    val copyStatement = query match {
      case None => "COPY %s TO STDOUT WITH CSV".format(table.tableName)
      case Some(q) => "COPY (%s) TO STDOUT WITH CSV".format(q)
    }

    println("READING FROM %s".format(copyStatement))

    val pipedWriter = new PipedOutputStream
    val pipedReader = new PipedInputStream(pipedWriter)

    Future {
      for(con <- managed(connection(table)))
        getCopyApi(con).copyOut(copyStatement, new OutputStreamWriter(pipedWriter))

      println("DONE READING FROM %s".format(copyStatement))
    }

    new InputStreamReader(pipedReader)
  }
}

object CopyDatabaseHSPECEmitter {
  val system = ActorSystem("DbSystem", ConfigFactory.load("akka.conf"))
}

class CopyDatabaseHSPECEmitter @Inject() (val jobRequest: JobRequest, val csvSerializer: CSVSerializer) extends Emitter[HSPEC] with Logging with DBAccessor {
  import CopyDatabaseHSPECEmitter._

  implicit val timeout: Timeout = Timeout(10 minutes) // needed for `?` below

  println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> COPYING partition")
  val table = jobRequest.hspecDestinationTableName
  val copyStatement = "COPY %s FROM STDIN WITH CSV".format(table.tableName)

  implicit val con = connection(table)
  con.setAutoCommit(false)
  val copyApi = getCopyApi(con)


  val meta = con.getMetaData()
  val indexInformation = meta.getIndexInfo(con.getCatalog(), "public", table.tableName, false, true);
  println("INDEX INFO %s".format(indexInformation))

  while (indexInformation.next()) {
    val dbIndexName = indexInformation.getString("index_name");
    println("DROPPING INDEX %s".format(dbIndexName))
    execute("drop index %s".format(dbIndexName))
  }
  indexInformation.close

  val tableSpace = (query("select tablespace from pg_tables where tablename = 'hspec_suitable10'") {
    rs =>
      rs.next
      rs.getString("tablespace")
  }).get

  execute("truncate %s".format(table.tableName))

  class DatabaseWriter extends Actor {
    val pipedWriter = new PipedOutputStream

    val tableWriter = new TableWriter[HSPEC] { def writer = new OutputStreamWriter(pipedWriter) }
    val sink = new CSVPositionalSink(tableWriter)
    val csvEmitter = new CSVEmitter(sink, csvSerializer)

    def receive = {
      case r : HSPEC =>
        csvEmitter.emit(r)
      case "Writer" => sender ! pipedWriter
      case "Wait" => csvEmitter.flush; sender ! "ok"
      case _ => // ignore
    }

    override def postStop = {pipedWriter.close(); println("WRITER CLOSED")}
  }

  val writer = system.actorOf(Props(new DatabaseWriter).withDispatcher("db-writer-dispatcher"))

  class DatabaseReader(val pipedWriter: PipedOutputStream) extends Actor {
    val pipedReader = new PipedInputStream(pipedWriter)

    def receive = {
      case "Start" => {
        println("STARTING DB COPY %s".format(java.lang.Thread.currentThread.getId))
        copyApi.copyIn(copyStatement, pipedReader)
        println("DB COPY FINISHED %s".format(java.lang.Thread.currentThread.getId))
        con.commit
        println("COMMITED")
      }
      case "Wait" => sender ! "ok"
      case _ => // ignore
    }

    override def postStop = {pipedReader.close() ; println("READER CLOSED")}
  }

  import akka.dispatch.Await
  val pipedWriter = Await.result((writer ask "Writer").mapTo[PipedOutputStream], 10 seconds)

  val reader = system.actorOf(Props(new DatabaseReader(pipedWriter)).withDispatcher("db-writer-dispatcher"))
  reader ! "Start"

  def emit(r: HSPEC) = {
    writer ! r
  }

  def flush = {
    println("FLUSHING")
    Await.result(writer ask "Wait", 10 minutes)
    println("writer finished")
    system.stop(writer)
    println("writer, stopped")

    Await.result(reader ask "Wait", 10 minutes)
    println("reader finished")
    system.stop(reader)
    println("reader stopped")

    println("VACUUM %s".format(table.tableName))
    con.setAutoCommit(true)
    execute("vacuum %s".format(table.tableName))

    println("recreating indices for %s".format(table.tableName))

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    def createIndex(field: String) = Future {
      implicit val con = connection(table)

      val ts = tableSpace match {
        case "" => ""
        case x => "TABLESPACE %s".format(x)
      }
      val sql = "CREATE INDEX %s_%s_idx ON %s USING btree (%s) %s;".format(table.tableName, field, table.tableName, field, ts)
      println("CREATING INDEX: %s".format(sql))
      execute(sql)
      println("INDEX CREATED:  %s".format(sql))
    }

    val indices = List("csquarecode", "probability", "speciesid").map(createIndex)

    Await.result(Future.sequence(indices), 10 hours)

    println("DONE")
    con.close
  }
}
