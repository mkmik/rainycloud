package it.cnr.aquamaps.cloud

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.Dispatchers
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._

import com.google.gson.Gson
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.querulous.evaluator.Transaction
import com.twitter.querulous.query.NullValues
import it.cnr.aquamaps._

import com.google.inject._
import it.cnr.aquamaps.jdbc.LiteDataSource
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import com.weiglewilczek.slf4s.Logging


