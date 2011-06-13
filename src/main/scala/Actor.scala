package it.cnr.aquamaps

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.Dispatchers
import akka.util.duration._

object CompatActor {
  def actor(body: => Receive): ActorRef = actorOf(new Actor {
    def receive() = body
  }).start()

  def tactor(body: => Receive): ActorRef = actorOf(new Actor {
    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 15, 100.milliseconds)
    def receive() = body
  }).start()

  def react(body: => Receive) = body
  def receive(body: => Receive) = body
}
