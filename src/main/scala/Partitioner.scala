package it.cnr.aquamaps

import io.Source.fromFile

import com.google.inject._
import net.lag.configgy.Config

/*!
 
 A partitioner creates a partition of the key space. It returns partitions which are describes by ranges over keys.
 */
trait Partitioner {
  def partitions: Iterator[Partition]
}

@serializable
case class Partition(val start: String, val size: Long)


/*!

 The simples partitioner is the StaticPartitioner, which simply reads key ranges from a precomputed file
 */
class StaticPartitioner @Inject() (val ranges: Iterator[String]) extends Partitioner {
  def toPartition(line: String) = {
    val Array(s, k) = line.trim split " "
    Partition(k, s.toInt)
  }

  def partitions = ranges map toPartition
}
