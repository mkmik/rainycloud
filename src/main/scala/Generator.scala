package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject

object HSPECGenerator extends Worker {
	private val log = Logger.getLogger(this.getClass);
	
	def run(task: JSONObject) {
		val payload = task.get("args")
		log.info("hspen task: " + task)
	}
}
