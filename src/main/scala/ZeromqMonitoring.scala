package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

class ZeromqMonitoring extends ScalatraServlet with ScalateSupport with UrlSupport {
  val log = Logger(classOf[ZeromqMonitoring])

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = "/submitter"

  def render(content: Seq[Node]) = {
    <html>
      <head>
        <title>Rainy cloud</title>
        <style>{ style }</style>
      </head>
      <body>
        <a href={ url("/") }>Home</a>
        <a href={ url("/submit-test") }>Submit test</a>
        <hr/>
        { content }
      </body>
    </html>
  }

  get("/") {
    render(<div>
             <table>
               <tr>
                 <th>Id</th>
                 <th>Submitted</th>
                 <th>Tasks</th>
                 <th>Status</th>
                 <th>Actions</th>
               </tr>
               {
                 for ((id, job) <- Submitter.jobs()) yield <tr>
                                                             <td>{ id }</td>
                                                             <td>Unknown</td>
                                                             <td>{ job.completedTasks }/{ job.totalTasks }</td>
                                                             <td>{ if (job.completed) "Completed" else "Running" }</td>
                                                             <td>
                                                               {
                                                                 if (job.completed)
                                                                   <a href={ url("/job/%s/delete".format(id)) }>Delete</a>
                                                                 else
                                                                   <a href={ url("/job/%s/kill".format(id)) }>Kill</a>
                                                               }
                                                             </td>
                                                           </tr>
               }
             </table>
           </div>)
  }

  get("/submit-test") {
    log.info("submitting test jobs")
    Submitter.spawnTest()
    redirect(url("/"))
  }

  get("/job/:job/delete") {
    Submitter.deleteJob(params("job"))
    redirect(url("/"))
  }

}
