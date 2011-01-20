package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject

trait Worker {
  def run(task: JSONObject)
}
