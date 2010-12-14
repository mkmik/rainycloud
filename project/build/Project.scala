import sbt._

class RainyCloudProject(info: ProjectInfo) extends DefaultProject(info) {
  val log4j = "log4j" % "log4j" % "1.2.16"

	//val scromiumRepo = "Cliff's Scromium Repo" at "http://cliffmoon.github.com/scromium/repository/"

	//	val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  //val scromium = "scromium" % "scromium_2.8.0" % "0.6.4" // artifacts Artifact("scromium-all_2.8.0", "all", "jar")


	val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
	val metrics = "com.yammer" %% "metrics" % "1.0.7" withSources()

}
