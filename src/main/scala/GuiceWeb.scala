package it.cnr.aquamaps
import com.google.inject.Guice
import com.google.inject.Injector
import com.google.inject.servlet.GuiceServletContextListener
import com.google.inject.servlet.ServletModule

class WebServletModule extends ServletModule {
  override def configureServlets() = {
    serve("/submitter/*").`with`(classOf[ZeromqMonitoring])
    serve("/*").`with`(classOf[cloud.SubmitterApi])
  }
}

class GuiceServletConfig extends GuiceServletContextListener {

  def getInjector(): Injector = Guice.createInjector(new WebServletModule())
}

