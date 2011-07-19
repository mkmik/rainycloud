package it.cnr.aquamaps
import it.cnr.aquamaps.cloud._

import com.google.inject.Guice
import com.google.inject.Injector
import com.google.inject.servlet.GuiceServletContextListener
import com.google.inject.servlet.ServletModule

import com.google.inject.util.{ Modules => GuiceModules }

case class WebServletModule() extends ServletModule {
  override def configureServlets() = {
    serve("/submitter/*").`with`(classOf[ZeromqMonitoring])
    serve("/*").`with`(classOf[cloud.SubmitterApi])

  }
}

class GuiceServletConfig extends GuiceServletContextListener {

  def getInjector(): Injector = Guice.createInjector((GuiceModules `override` WebServletModule() `with` WebModule()))

}

