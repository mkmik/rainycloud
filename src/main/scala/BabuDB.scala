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
import uk.me.lings.scalaguice.ScalaModule

case class BabuDBModule() extends AbstractModule with ScalaModule {
  def configure() {
    bind[Fetcher[HCAF]].to[BabuDBFetcher[HCAF]].in[Singleton]
  }
}

/*!
 Embedded key/value store. More efficient than naive memory db, but local, thus in our scenario the initial data has to be loaded from another loader.
 */
class BabuDBFetcher[A <: Keyed] @Inject() (val loader: Loader[A]) extends Fetcher[A] {

  implicit def runnable(f: () => Unit): Runnable =
    new Runnable() { def run() = f() }

  val databaseSystem = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath("/tmp/babudb").setLogAppendSyncMode(DiskLogger.SyncMode.ASYNC).build())
  val dbman = databaseSystem.getDatabaseManager();

  val db = getDb

  println("opening db %s".format(db))

  @PostConstruct
  def init {
    println("GOT EVENT")
  }

  @PreDestroy
  override
  def shutdown {
    println("Shutting down %s".format(this))
    db.shutdown
    databaseSystem.shutdown()
  }

  def fetch(start: String, size: Long) = {
    val res = db.prefixLookup(0, start.getBytes, null)
    res.get.take(size.toInt).map { x => deserialize(x.getValue) }.toIterable
  }

  def getDb = {
    try {
      dbman.createDatabase("myDB", 1)
      val db = dbman.getDatabase("myDB");
      reload(db)
      db
    } catch {
      case _: BabuDBException => dbman.getDatabase("myDB");
    }
  }

  def reload(db: Database) = {
    for (record <- loader.load)
      db.singleInsert(0, record.key.getBytes, serialize(record), null)
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
