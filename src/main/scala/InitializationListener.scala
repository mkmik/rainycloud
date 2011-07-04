package it.cnr.aquamaps
import java.io.StringWriter

import javax.servlet._
import net.lag.configgy.Configgy
import org.apache.commons.io.IOUtils

class InitializationListener extends ServletContextListener {
	def contextDestroyed(sce: ServletContextEvent) {
  } 

  def contextInitialized(sce: ServletContextEvent) { 
    //Configgy.configure("/home/marko/Projects/rainycloud/rainycloud.conf")
    val conf = getClass().getResourceAsStream("/rainycloud.conf")
    println("GOT CONF %s".format(conf));
    
//    Configgy.configure(conf)
    val confSource = new StringWriter()
    IOUtils.copy(conf, confSource)
    Configgy.configureFromString(confSource.toString)
    //Configgy.configureFromResource("/rainycloud.conf")

    println("CONTEXT INITIALIZED, config file loaded")

  }
}
