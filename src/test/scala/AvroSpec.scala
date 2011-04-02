package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito
import org.mockito.Matchers._ // to use matchers like anyInt()

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import com.googlecode.avro.marker._


object AvroSpec extends Specification with Mockito {
  "avro" should {
    "serialize and deserialize" in {
      val record = HCAF.makeHcaf()
      val serialized = record.toBytes

      val deserialized = HCAF.makeHcaf().parse(serialized)

      deserialized must beEqual(record)
    }
  }
}
