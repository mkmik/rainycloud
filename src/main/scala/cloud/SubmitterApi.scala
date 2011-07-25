package it.cnr.aquamaps.cloud
import com.google.gson.Gson
import it.cnr.aquamaps._

import javax.servlet.Servlet

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import com.weiglewilczek.slf4s.Logging
import net.lag.configgy.{ Config, Configgy }

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

case class WebModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {

    bind[Submitter].in[Singleton]
    bind[SubmitterApi].in[Singleton]
    bind[JobSubmitter].to[ZeromqJobSubmitter].in[Singleton]
    bind[ZeromqMonitoring].in[Singleton]
  }
}

case class Table(val jdbcUrl: String, var tableName: String)

case class JobRequest(
  val environment: String,
  val generativeModel: String,
  val hcafTableName: Table,
  val hspenTableName: Table,
  val hspecDestinationTableName: Table,
  val is2050: Boolean,
  val isNativeGeneration: Boolean,
  val nWorkers: Int,
  val occurrenceCellsTable: Table,
  val userName: String,
  val configuration: java.util.Map[String, String])

class SubmitterApi @Inject() (val launcher: Launcher, val submitter: Submitter) extends ScalatraServlet with ScalateSupport with UrlSupport with Logging {
  import JobSubmitter.Job

  beforeAll {
    contentType = "application/json"
  }

  override def contextPath = getServletConfig().getServletContext().getContextPath() + "/api"

  val style = """ """

  get("/") {
    redirect(getServletConfig().getServletContext().getContextPath() + "/submitter/")
  }

  val gson = new Gson()

  post("/submit") {
    //    logger.info("posted %s".format(request.body))

    val req = gson.fromJson(request.body, classOf[JobRequest])
    logger.info("parsed json %s".format(req))

    launcher.launch(req)

    //val job = SubmitterTester.spawnTest()
    //val id = job.id
    val id = "123"
    """{"error": null,"id": "%s"}""".format(id)

  }

  /* return status for monitoring graph */
  get("/status/:id") {
    val id = params("id")
    val job = submitter.jobs().get(id)
    job match {
      case None =>
        """{"error" : "unknown job"}"""
      case Some(job) =>
        val status = if (job.completed) "DONE" else "RUNNING"
        val metrics = if (job.completed) "{}" else buildMetrics(job)
        val completion = (job.completedTasks: Double) * 100.0 / job.totalTasks

        """{"id":"%s","status":"%s","completion":%s,"metrics":%s}""".format(id, status, completion, metrics)
    }
  }

  def buildMetrics(job: Job) = {
    """ {"load":[{"resId":"W0","value":82.374146}],"throughput":[1308157425388,2722647]} """
  }

}

