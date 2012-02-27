package it.cnr.aquamaps.cloud

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.Dispatchers
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._

import com.google.inject._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }
import uk.me.lings.scalaguice.InjectorExtensions._

import com.weiglewilczek.slf4s.Logging
import it.cnr.aquamaps.jdbc.LiteDataSource

import it.cnr.aquamaps._


object CopyDatabaseHSPECEmitter {
  val system = ActorSystem("DbSystem")
}

class CopyDatabaseHSPECEmitter @Inject() (val jobRequest: JobRequest, val csvSerializer: CSVSerializer) extends Emitter[HSPEC] with Logging {
  import CopyDatabaseHSPECEmitter._

  implicit val timeout: Timeout = Timeout(10 minutes) // needed for `?` below

  println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> COPYING partition")
  val table = jobRequest.hspecDestinationTableName
  val copyStatement = "COPY %s FROM STDIN WITH CSV".format(table.tableName)

  import akka.util.duration._
  import java.io._
  import java.sql.DriverManager
  import org.postgresql.PGConnection
  Class.forName("org.postgresql.Driver")

  val urlComps = table.jdbcUrl.split(";")
  val cleanUrl = urlComps(0)
  val user = urlComps(1).split("=")(1)
  val password = urlComps(2).split("=")(1)

  val con = DriverManager.getConnection(cleanUrl, user, password)
  val pgcon = con.asInstanceOf[PGConnection]
  val copyApi = pgcon.getCopyAPI()


  //con.execute("drop index hspec2011_11_29_10_18_22_135_idx");
  //con.execute("truncate hspec2011_11_29_10_18_22_135");

  class DatabaseWriter extends Actor {
//    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, mailboxCapacity = 10)

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

  val writer = system.actorOf(Props(new DatabaseWriter))

  class DatabaseReader(val pipedWriter: PipedOutputStream) extends Actor {
//    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, mailboxCapacity = 10)

    val pipedReader = new PipedInputStream(pipedWriter)

    def receive = {
      case "Start" => {
        println("STARTING DB COPY %s".format(java.lang.Thread.currentThread.getId))
        copyApi.copyIn(copyStatement, pipedReader)
        println("DB COPY FINISHED %s".format(java.lang.Thread.currentThread.getId))
      }
      case "Wait" => sender ! "ok"
      case _ => // ignore
    }

    override def postStop = {pipedReader.close() ; println("READER CLOSED")}
  }

  import akka.dispatch.Await
  val pipedWriter = Await.result((writer ask "Writer").mapTo[PipedOutputStream], 10 seconds)

  val reader = system.actorOf(Props(new DatabaseReader(pipedWriter)))
  reader ! "Start"

  def emit(r: HSPEC) = {
    writer ! r
  }

  def flush = {
    println("FLUSHING")
    writer ask "Wait"
    println("writer finished")
    system.stop(writer)
    println("writer, stopped")

    reader ask "Wait"
    println("reader finished")
    system.stop(reader)
    println("reader stopped")

    println("DONE")
  }
}
