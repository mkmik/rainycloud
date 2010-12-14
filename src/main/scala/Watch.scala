package it.cnr.aquamaps

import stopwatch.web.Server


import stopwatch.Stopwatch
import stopwatch.StopwatchGroup
import stopwatch.StopwatchRange

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
