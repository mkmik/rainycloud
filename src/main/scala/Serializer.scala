package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._

import com.googlecode.avro.marker._

import java.io._
import resource._

/*! Let's define a way to serialize our records in a compact byte stream. */
trait Serializer[A] {
  def serialize(record: A): Array[Byte]
  def deserialize(bytes: Array[Byte]): A
}

/*! Avro is a very fast and compact serializer which has some rough edges. In particular records have
 to implement the AvroRecord interface, have only mutable fields, and provide an empty constructor. */
abstract class AvroSerializer[A <: AvroRecord] @Inject() extends Serializer[A] {
  def makeRecord: A
  def serialize(record: A): Array[Byte] = record.toBytes
  def deserialize(bytes: Array[Byte]): A = makeRecord.parse(bytes)
}

/*! We use implicits to get an empty record when needed */
object AvroSerializer {
  def apply[A <: AvroRecord]()(implicit a: A): AvroSerializer[A] = new AvroSerializer[A] { def makeRecord = a }
}

/*! Otherwise we can rely on good old java serialization */
class SerializableSerializer[A] @Inject() extends Serializer[A] {
  def serialize(record: A): Array[Byte] = {
    val baos = new ByteArrayOutputStream(1024)
    val res = for (o <- managed(new ObjectOutputStream(baos)))
      o writeObject record

    baos.toByteArray
  }

  def deserialize(bytes: Array[Byte]): A = {
    val res = for (o <- managed(new ObjectInputStream(new ByteArrayInputStream(bytes))))
      yield o.readObject.asInstanceOf[A]
    res.opt.get
  }
}
