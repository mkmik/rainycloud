package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import net.lag.configgy.{ Config, Configgy }

import com.google.inject._


class VenusGui @Inject() (val submitter: Submitter) extends ScalatraServlet with ScalateSupport with UrlSupport {

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = getServletConfig().getServletContext().getContextPath() + "/JobManagement"

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
    render(<h2>Job Overview</h2>
           <table>
           <tr>
           <th>Job Owner</th>
           <th>ApplicationIdentificationURI</th>
           <th>CustomerJobID</th>
           <th>Status</th>
           <th>InstanceID</th>
           <th>Submission</th>
           <th>Last Change</th>
           <th>Status Text</th>
           <th>Stdout</th>
           <th>Stderror</th>
           </tr>
           
           <tr>
           <td> gcube </td>
           <td> http://www.cnr.eu/cloud/demo/RainyCloudApp58 </td>
           <td> customJobID_7/9/2012 1:27:01 PM </td>
           <td> Finished </td>
           <td> Cloud.WebRole_IN_0 </td>
           <td> 7/9/2012 11:27:08 AM </td>
           <td> 7/9/2012 11:27:34 AM </td>
           <td> Status</td>
           <td>  </td>
           <td>  </td>
           <td class="display-label">
           <label for="No_job-specific_actions_available">No job-specific actions available</label>        </td>
           </tr>
           </table>)
  }


}

