package org.apache.spark.sql.hive

import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ResubmitFailedStages
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable


private[spark] class HiveExternalMemoryCatalog(conf: SparkConf, hadoopConf: Configuration)
    extends HiveExternalCatalog(conf, hadoopConf) with Logging {

  val meta = new mutable.HashMap[String, CatalogTable]()
  val refreshTime = conf.get("spark.sql.hiveCatalog.memory.refreshTime", "600").toInt
  val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor("hive-memory-catalog-refresh")

  RefreshThreadStart()

  override def getTable(db: String, table: String): CatalogTable = {
    meta.synchronized {
      meta.getOrElseUpdate(s"db:$db-tbl:$table", {
        super.getTable(db, table)
      })
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {

    meta.synchronized {
      meta.remove(s"db:$db-tbl:$table")
    }
    super.dropTable(db, table, ignoreIfNotExists, purge)
  }

  private def RefreshThreadStart(): Unit = {
    executor.schedule(
      new Runnable {
        override def run(): Unit = meta.synchronized(meta.clear())
      },
      refreshTime,
      TimeUnit.SECONDS
    )
  }

}
