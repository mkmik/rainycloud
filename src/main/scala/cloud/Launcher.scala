package it.cnr.aquamaps.cloud
import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.actor.Channel
import akka.actor.PoisonPill
import akka.dispatch.Dispatchers
import com.google.gson.Gson
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.querulous.evaluator.Transaction
import com.twitter.querulous.query.NullValues
import it.cnr.aquamaps._

import com.google.inject._
import it.cnr.aquamaps.jdbc.LiteDataSource
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import com.weiglewilczek.slf4s.Logging

case class LauncherModule(val jobRequest: JobRequest, val jobSubmitter: JobSubmitter) extends AbstractModule with ScalaModule with RainyCloudModule with Logging {
  def configure() {
    logger.info("CONF: %s".format(conf.getString("hcafFile")))

    /*! This overrides the default `Generator` to use a specific wrapper for remote submission. The `CloudGenerator` converts the parameters into serializable params
     * and spawns task using a Submitter */
    bind[Generator].to[CloudGenerator]

    bind[Emitter[HSPEC]].to[CloudHSPECEmitter]

    bind[JobSubmitter].toInstance(jobSubmitter)
    bind[JobRequest].toInstance(jobRequest)

    bind[Loader[HSPEN]].to[DummyLoader[HSPEN]]
    bind[Loader[HCAF]].to[DummyLoader[HCAF]]

    @Provides
    @Singleton
    def job(jobSubmitter: JobSubmitter) = jobSubmitter.newJob
  }
}

class DummyLoader[A] extends Loader[A] {
  def load: Iterable[A] = List()
}

case class TaskRequest(val partition: Partition, val job: JobRequest)

class CloudGenerator @Inject() (val jobRequest: JobRequest, val job: JobSubmitter.Job, val submitter: Submitter) extends Generator with Logging {
  val gson = new Gson()

  def computeInPartition(p: Partition) = {
    logger.info("Cloud generator computing in partition %s".format(p))

    job.addTask(submitter.js.newTaskSpec(gson.toJson(TaskRequest(p, jobRequest))))
  }
}

class CloudHSPECEmitter @Inject() (val job: JobSubmitter.Job, val submitter: Submitter) extends Emitter[HSPEC] {
  def emit(record: HSPEC) = {} // dummy

  def flush = {
    job.seal()
    submitter.registerJob(job)
  }
}

/** a Launcher prepares the environment for the entry point so that it can use Submitter */
class Launcher @Inject() (val jobSubmitter: JobSubmitter) extends Logging {
  def launch(jobRequest: JobRequest) = {
    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (LauncherModule(jobRequest, jobSubmitter), HDFSModule()))

    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    logger.info("done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }
}

///

case class TaskLauncherModule(val taskRequest: TaskRequest) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    // doesn't override
    //    bind[TableWriter[HSPEC]].toInstance(new FileSystemTableWriter(conf.getString("hspecFile").getOrElse("/tmp/hspec.csv.gz")))
    //        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]].in[Singleton]

    bind[Emitter[HSPEC]].to[CopyDatabaseHSPECEmitter].in[Singleton]
    bind[Partitioner].toInstance(new StaticPartitioner(Seq("%s %s".format(taskRequest.partition.size, taskRequest.partition.start)).toIterator))

    bind[TaskRequest].toInstance(taskRequest)
  }
}

class CopyDatabaseHSPECEmitter @Inject() (val taskRequest: TaskRequest, val csvSerializer: CSVSerializer) extends Emitter[HSPEC] with Logging {
  println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> COPYING partition %s".format(taskRequest.partition))
  val table = taskRequest.job.hspecDestinationTableName
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


  class DatabaseWriter extends Actor {
    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, mailboxCapacity = 10)

    val pipedWriter = new PipedOutputStream

    val tableWriter = new TableWriter[HSPEC] { def writer = new OutputStreamWriter(pipedWriter) }
    val sink = new CSVPositionalSink(tableWriter)
    val csvEmitter = new CSVEmitter(sink, csvSerializer)

    def receive = {
      case r : HSPEC =>
        csvEmitter.emit(r)
      case "Writer" => self.reply(pipedWriter)
      case "Wait" => csvEmitter.flush; self.reply("ok")
      case _ => // ignore
    }

    override def postStop = {pipedWriter.close(); println("WRITER CLOSED")}
  }

  val writer = actorOf(new DatabaseWriter).start

  class DatabaseReader(val pipedWriter: PipedOutputStream) extends Actor {
    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, mailboxCapacity = 10)

    val pipedReader = new PipedInputStream(pipedWriter)

    def receive = {
      case "Start" => {
        println("STARTING DB COPY %s".format(java.lang.Thread.currentThread.getId))
        copyApi.copyIn(copyStatement, pipedReader)
        println("DB COPY FINISHED %s".format(java.lang.Thread.currentThread.getId))
      }
      case "Wait" => self.reply("ok")
      case _ => // ignore
    }

    override def postStop = {pipedReader.close() ; println("READER CLOSED")}
  }


  val pipedWriter = (writer !! "Writer") match {
    case Some(writer: java.io.PipedOutputStream) => writer
    case None => throw new Exception("cannot obtain piped writer")
  }

  val reader = actorOf(new DatabaseReader(pipedWriter)).start
  reader ! "Start"

  def emit(r: HSPEC) = {
    writer ! r
  }

  def flush = {
    println("FLUSHING")
    writer !! "Wait"
    println("writer finished")
    writer.stop()
    println("writer, stopped")

    reader !! "Wait"
    println("reader finished")
    reader.stop()
    println("reader stopped")

    println("DONE")
  }
}

object TaskLauncher extends Logging {
  val gson = new Gson()

  def launch(task: TaskRef, worker: ActorRef) = {
    println("---- launching task %s".format(task.id))

    val taskRequest = gson.fromJson(task.id, classOf[TaskRequest])

    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (TaskLauncherModule(taskRequest), HDFSModule()))
    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)

  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    logger.info("---- task done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }

}
