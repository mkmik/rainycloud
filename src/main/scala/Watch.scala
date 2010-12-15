package it.cnr.aquamaps

import stopwatch.web.Server


import stopwatch.Stopwatch
import stopwatch.StopwatchGroup
import stopwatch.StopwatchRange
import stopwatch.TimeUnit._

trait Watch {
	Watch.run

	Stopwatch.enabled = true
	Stopwatch.range = StopwatchRange(0 seconds, 15 seconds, 500 millis)
}

object Watch {
	val server = new stopwatch.web.Server
	
	def run = {
		// register StopwatchGroups you want to monitor
		server.groups ::= Stopwatch
		
		// configure port number
		server.port = 9999
		
		server.start()
	}
}
