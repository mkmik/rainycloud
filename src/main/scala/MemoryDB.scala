package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._

import Watch.timed

/*!
 Simple store which keeps records in memory. Extremely inefficient.
 */
class MemoryFetcher[A <: Keyed] @Inject() (val loader: Loader[A]) extends Fetcher[A] {

  val records = timed("preloading") {loader.load}

  def fetch(start: String, size: Long) = {
    val skip = records dropWhile {el => el.key < start}
    val page = skip take size.toInt
    page
  }
}
