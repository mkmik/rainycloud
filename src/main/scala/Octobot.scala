package it.cnr.aquamaps

/*!## Octobot
 
 This code is used only when running a scenario with the octobot workers
 */

import org.json.simple.JSONObject

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._

trait Bot {
  def processTask(task: JSONObject)
}

/*! We receive a partition (start + size) via JSON. We just convert the parameters from
 json to a partition instance and delegate the work to the Generator */
class HSPECGeneratorOctobot @Inject() (val generator: Generator) extends Bot {
  def processTask(task: JSONObject)  = generator.computeInPartition(partition(task.get("start"), task.get("size")))

  def partition(start: Any, size: Any) = new Partition(start.asInstanceOf[String], size.asInstanceOf[Long])
}

/*!## Entry point
 
 This is the octopbot entry point. 
 */
object HSPECGeneratorOctopus {

  /*! We construct the Guice injector only once and then reuse the instances. */
  val injector = Guice createInjector AquamapsModule()
  val bot = injector.instance[Bot]

  /*! Octobot will invoke the 'run' static method for each incoming job */
  def run(task: JSONObject) = bot.processTask(task)
}
