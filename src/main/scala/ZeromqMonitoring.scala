package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import com.weiglewilczek.slf4s.Logging
import net.lag.configgy.{ Config, Configgy }

import com.google.inject._

class ZeromqMonitoring @Inject() (val submitter: Submitter) extends ScalatraServlet with ScalateSupport with UrlSupport with Logging {

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = getServletConfig().getServletContext().getContextPath() + "/submitter"

  def render(content: Seq[Node]) = {
    <html>
      <head>
        <link href={ url("/stylesheets/site.css") } rel="stylesheet" type="text/css"/>
        <script src={ url("/javascripts/jquery-1.6.1.min.js") } type="text/javascript"></script>
        <script src={ url("/javascripts/app.js") } type="text/javascript"></script>
        <title>Rainy cloud</title>
        <style>{ style }</style>
      </head>
      <body>
        <div class="page">
          <div id="header">
            <div id="title"><h1>VENUS-C Management Application</h1></div>
            <div id="logindisplay">[<a href={ url("/login") }>Log On</a>]</div>
            <div id="menucontainer">
              <ul id="menu">
                <li>
                  <a href={ url("/") }>Home</a>
                </li>
                <li>
                  <a href={ url("/submit-test") }>Submit test</a>
                </li>
              </ul>
            </div>
          </div>
          <div id="main">
            { content }
          </div>
        </div>
      </body>
    </html>
  }

  def renderFile(name: String, ct: String = "text/css") = {
    contentType = ct
    io.Source.fromInputStream(getClass().getResourceAsStream(name)).mkString
  }

  get("/stylesheets/site.css") { renderFile("/stylesheets/site.css") }
  get("/javascripts/:name") { renderFile("/javascripts/" + params("name"), "text/javascript") }

  get("/") {
    render(<div>
             <h2>Spool</h2>
             <table>
               <tr>
                 <th>Queue length</th>
               </tr>
               <tr>
                 <td>
                   { submitter.queueLength }
                 </td>
               </tr>
             </table>
             <h2>Workers</h2>
             <table>
               <tr>
                 <th>Id</th>
                 <th>Completed tasks</th>
                 <th>Last heartbeat</th>
                 <th>Uptime</th>
               </tr>
               {
                 for ((id, worker) <- submitter.workers) yield <tr>
                                                                 <td>{ id }</td>
                                                                 <td>{ worker.completed }</td>
                                                                 <td>{ worker.heartbeatAgo / 1000 }s ago</td>
                                                                 <td>{ Utils.approximateTime(worker.uptime) }</td>
                                                               </tr>
               }
             </table>
             <h2>Jobs</h2>
             <table>
               <tr>
                 <th>Id</th>
                 <th>Submitted</th>
                 <th>Tasks</th>
                 <th>Status</th>
                 <th>Actions</th>
               </tr>
               {
                 for ((id, job) <- submitter.jobs()) yield <tr>
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
    logger.info("submitting test jobs")
    //submitterTester.spawnTest()
    redirect(url("/"))
  }

  get("/job/:job/delete") {
    submitter.deleteJob(params("job"))
    redirect(url("/"))
  }

  get("/job/:job/kill") {
    submitter.killJob(params("job"))
    redirect(url("/"))
  }

}

