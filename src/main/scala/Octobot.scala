package it.cnr.aquamaps

import org.json.simple.JSONObject
import org.json.simple.JSONArray

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._

trait Bot {
  def processTask(task: JSONObject)
}

class HSPECGeneratorOctobot @Inject() (val generator: Generator) extends Bot {
  def processTask(task: JSONObject) {
    val start = task.get("start").asInstanceOf[String]
    val size = task.get("size").asInstanceOf[Long]

    generator.computeInPartition(new Partition(start, size))
  }
}

object HSPECGeneratorOctopus extends Worker {

  val injector = Guice createInjector AquamapsModule()
  val bot = injector.instance[Bot]

  def run(task: JSONObject) = bot.processTask(task)
}
