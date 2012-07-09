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
import scala.collection.JavaConversions._
import com.google.gson.Gson
import com.google.gson.reflect._


class VenusGui @Inject() (val submitter: Submitter) extends ScalatraServlet with ScalateSupport with UrlSupport {
  import JobSubmitter.Job

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
           {renderJobs()}
           </table>)
  }

  def formatDate(date: java.util.Date) = new java.text.SimpleDateFormat().format(date)

  class JobReport {
    var completed: Boolean = false
    var error: String = ""
    var completion: Double = 0.0
    var startTime: String = ""

    def parsedStartedTime = parsedTime(startTime)

    def parsedTime(str: String) =  new java.text.SimpleDateFormat("M d, y h:m:s a").parse(str.replace("Jan", "1").replace("Feb", "2").replace("Mar", "3").replace("Apr", "4").replace("May", "5").replace("Jun", "6").replace("Jul", "7").replace("Aug", "8").replace("Sep", "9").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12"))

    def windowsStartedTime = formatDate(parsedStartedTime)
  }

  def renderJobs() = {
    val jobs = submitter.jobs()
    def jobDetail(j: Job) = {
      var completion = j.completedTasks match {
          case -1 => 100.0
          case x => math.min(95.0, (x: Double) * 100.0 / j.totalTasks)
        }

      val rps = j.completedTasks * 1000.0 / (System.currentTimeMillis - j.startTime)
      val eta = (System.currentTimeMillis + (1000.0 * (j.totalTasks - j.completedTasks) / rps).toInt)

      val res = new JobReport
      res.completed = j.completed
      res.error = j.error.getOrElse("")
      res.startTime = (new java.util.Date(j.startTime)).toString

      res
    }
    val map = jobs mapValues jobDetail

/*
    val gson = new Gson()
    object MyMap extends TypeToken[java.util.Map[String, java.util.Map[String, String]]]
    val oldJobs: java.util.Map[String, java.util.Map[String, String]]  = gson.fromJson(new java.io.FileReader("persistenJobList.json"), MyMap.getType)
    println("OLD JOBS %s".format(oldJobs))

    val fixedOldJobs = oldJobs mapValues {
      oldJob => oldJob + ("startTime" -> formatDate(new java.util.Date(1341844910290L)))
    }
    val mapMerged = (fixedOldJobs mapValues (_.toMap)).toMap ++ map
*/

    val mapMerged = map
    for((key, value) <- mapMerged)
      yield renderTasks(key, value)
  }

  def renderTasks(uuid: String, value: JobReport) = {
    for(i <- scala.util.Random.shuffle((0 to 20).toList))
      yield <tr>
    <td> pasquale.pagano </td>
    <td> http://www.cnr.eu/cloud/demo/RainyCloudApp58 </td>
    <td> aquamaps_{uuid}_{i} </td>
    <td> {if(value.completed) "Finished" else "Running"} </td>
    <td> Cloud.WebRole_IN_{i} </td>
    <td> {value.startTime} </td>
    <td> 7/9/2012 11:27:34 AM </td>
    <td> Status {getDummyStatus(i, uuid, value)} Stdout {if(value.completed) getDummyStdout(i, uuid, value) else ""} Stderr {if(value.completed) getDummyStderr(i, uuid, value) else ""} </td>
    <td>  </td>
    <td>  </td>
    <td class="display-label">
    <label for="No_job-specific_actions_available">No job-specific actions available</label>        </td>
    </tr>
  }

  def getDummyStatus(i: Int, uuid: String, value: JobReport) = {
    val pseudoUuid = "%s_%s".format(uuid, i)
    val taskGuid = new java.util.UUID(pseudoUuid.hashCode()* 125123125124L, pseudoUuid.hashCode()* 66551241512L)

    val pseudoWorker = "%sgqwga%ssgoiqwg%s".format(i, i, i)
    val workerGuid = new java.util.UUID(pseudoWorker.hashCode()* 125123125124L, pseudoWorker.hashCode()* 66551241512L)

    val res = """
job-##GUID## Fetching job took 00:13:33.4422671 job-##GUID## Started execution job-##GUID## Running job-##GUID## User created: gw000001 job-##GUID## Application installed job-##GUID## Download took 00:00:10.0147109 job-##GUID## The command line is job-##GUID## Working directory "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##":> Executable "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat" Args " "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz " job-##GUID## process.StartInfo.WorkingDirectory C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID## job-##GUID## process.StartInfo.FileName C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## process.StartInfo.Arguments "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz job-##GUID## process.StartInfo.LoadUserProfile False job-##GUID## process.StartInfo.UserName job-##GUID## process.StartInfo.Domain job-##GUID## process.StartInfo.Password XMZzQeSPjV2V123.- job-##GUID## process.StartInfo.RedirectStandardError True job-##GUID## process.StartInfo.RedirectStandardInput False job-##GUID## process.StartInfo.RedirectStandardOutput True job-##GUID## process.StartInfo.UseShellExecute False job-##GUID## process.StartInfo.Verb job-##GUID## process.StartInfo.WindowStyle Normal job-##GUID## process.EnableRaisingEvents True job-##GUID## process.StartInfo.EnvironmentVariables["jobId"] ##JOBID## job-##GUID## process.StartInfo.EnvironmentVariables["jobSubmissionEndpoint"] http://localhost/AcceptLocalJobs/Cloud.WebRole_IN_##I## job-##GUID## Application ended: C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## Upload took 00:00:09.9486023"""

    res.replaceAll("##GUID##", taskGuid.toString).replaceAll("##I##", "%s".format(i)).replaceAll("##WORKERGUID##", "%s".format(workerGuid)).replaceAll("##JOBID##", "aquamaps_%s_%s".format(uuid, i))
  }

  def getDummyStdout(i: Int, uuid: String, value: JobReport) = {
    val pseudoUuid = "%s_%s".format(uuid, i)
    val taskGuid = new java.util.UUID(pseudoUuid.hashCode()* 125123125124L, pseudoUuid.hashCode()* 66551241512L)

    val pseudoWorker = "%sgqwga%ssgoiqwg%s".format(i, i, i)
    val workerGuid = new java.util.UUID(pseudoWorker.hashCode()* 125123125124L, pseudoWorker.hashCode()* 66551241512L)

    val res = """
Status job-##GUID## Fetching job took 00:13:33.4422671 job-##GUID## Started execution job-##GUID## Running job-##GUID## User created: gw000001 job-##GUID## Application installed job-##GUID## Download took 00:00:10.0147109 job-##GUID## The command line is job-##GUID## Working directory "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##":> Executable "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat" Args " "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz " job-##GUID## process.StartInfo.WorkingDirectory C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID## job-##GUID## process.StartInfo.FileName C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## process.StartInfo.Arguments "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz job-##GUID## process.StartInfo.LoadUserProfile False job-##GUID## process.StartInfo.UserName job-##GUID## process.StartInfo.Domain job-##GUID## process.StartInfo.Password XMZzQeSPjV2V123.- job-##GUID## process.StartInfo.RedirectStandardError True job-##GUID## process.StartInfo.RedirectStandardInput False job-##GUID## process.StartInfo.RedirectStandardOutput True job-##GUID## process.StartInfo.UseShellExecute False job-##GUID## process.StartInfo.Verb job-##GUID## process.StartInfo.WindowStyle Normal job-##GUID## process.EnableRaisingEvents True job-##GUID## process.StartInfo.EnvironmentVariables["jobId"] ##JOBID## job-##GUID## process.StartInfo.EnvironmentVariables["jobSubmissionEndpoint"] http://localhost/AcceptLocalJobs/Cloud.WebRole_IN_##I## job-##GUID## Application ended: C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## Upload took 00:00:09.9486023 Stdout C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>set myPath=C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\ C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>copy C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\rainycloud.conf .\rainycloud.conf 1 file(s) copied. C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\jre6\bin\java -jar C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\rainycloud_2.8.1-assembly-1.0.jar "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>set myPath=
    """

    res.replaceAll("##GUID##", taskGuid.toString).replaceAll("##I##", "%s".format(i)).replaceAll("##WORKERGUID##", "%s".format(workerGuid)).replaceAll("##JOBID##", "aquamaps_%s_%s".format(uuid, i))
  }

  def getDummyStderr(i: Int, uuid: String, value: JobReport) = {
    val res = """Stderr INF [20110614-15:03:30.381] aquamaps: Available modules: BabuDBModule(), COMPSsModule(), COMPSsObjectModule(), HDFSModule() INF [20110614-15:03:30.381] aquamaps: Enabled modules: INF [20110614-15:03:38.864] aquamaps: executed partition %s in 1296ms INF [20110614-15:03:38.864] aquamaps: done"""

    res.format(i*1512 % 20)
  }


}
