package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._

import org.xtreemfs.babudb._
import org.xtreemfs.babudb.config._
import org.xtreemfs.babudb.log._
import org.xtreemfs.babudb.lsmdb._

import java.io._
import javax.annotation._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import com.google.inject.name.Names.named
import uk.me.lings.scalaguice.ScalaModule

case class BabuDBModule() extends AbstractModule with ScalaModule {
  def configure() {
    bind[Loader[HSPEN]].annotatedWith(named("forBabu")).to(classOf[TableHSPENLoader]).in(classOf[Singleton])
  }

  @Provides @Singleton
  def hcafFetcher(loader: Loader[HCAF]): Fetcher[HCAF] = new BabuDBFetcher("hcaf", loader)
  @Provides @Singleton
  def hspenLoader(@Named("forBabu") loader: Loader[HSPEN]): Loader[HSPEN] = new BabuDBLoader("hspen", loader)
}

trait BabuDB[A <: Keyed] {
  val loader: Loader[A]
  val dbName: String

  val databaseSystem = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath("/tmp/babudb").setLogAppendSyncMode(DiskLogger.SyncMode.ASYNC).build())
  val dbman = databaseSystem.getDatabaseManager();

  val db = getDb

  def stop {
    db.shutdown
    databaseSystem.shutdown()
  }

  def getDb() = {
    try {
      dbman.createDatabase(dbName, 1)
      val db = dbman.getDatabase(dbName);
      reload(db)
      db
    } catch {
      case _: BabuDBException => dbman.getDatabase(dbName);
    }
  }

  def reload(db: Database) = {
    print("caching %s ...".format(loader))
    for (record <- loader.load)
      db.singleInsert(0, record.key.getBytes, serialize(record), null)
    databaseSystem.getCheckpointer().checkpoint()
    println(" done")
  }

  def serialize(record: A) = {
    val baos = new ByteArrayOutputStream(1024)
    val o = new ObjectOutputStream(baos)

    o writeObject record
    val bar = baos.toByteArray
    o.close
    bar
  }

  def deserialize(bytes: Array[Byte]): A = {
    val o = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val res = o.readObject.asInstanceOf[A]
    o.close
    res
  }

}

/*!
 Embedded key/value store. More efficient than naive memory db, but local, thus in our scenario the initial data has to be loaded from another loader.
 */
class BabuDBFetcher[A <: Keyed] @Inject() (val dbName: String, val loader: Loader[A]) extends Fetcher[A] with BabuDB[A] {

  def fetch(start: String, size: Long) = {
    val res = db.prefixLookup(0, start.getBytes, null)
    res.get.take(size.toInt).map { x => deserialize(x.getValue) }.toIterable
  }

  override def shutdown = stop
}

class BabuDBLoader[A <: Keyed] @Inject() (val dbName: String, val loader: Loader[A]) extends Loader[A] with BabuDB[A] {

  def load = {
    val res = db.prefixLookup(0, "".getBytes, null)
    res.get.map { x => deserialize(x.getValue) }.toIterable
  }

  override def shutdown = stop
}
