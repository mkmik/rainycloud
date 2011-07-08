package it.cnr.aquamaps
import java.io.StringWriter

import javax.servlet._
import net.lag.configgy.Configgy
import org.apache.commons.io.IOUtils

class InitializationListener extends ServletContextListener {
	def contextDestroyed(sce: ServletContextEvent) {
  } 

  def contextInitialized(sce: ServletContextEvent) { 
    val conf = getClass().getResourceAsStream("/rainycloud.conf")
    val confSource = new StringWriter()
    IOUtils.copy(conf, confSource)
    Configgy.configureFromString(confSource.toString)

    println("RainyCloud 1.2 CONTEXT INITIALIZED, config file loaded")

  }
}
