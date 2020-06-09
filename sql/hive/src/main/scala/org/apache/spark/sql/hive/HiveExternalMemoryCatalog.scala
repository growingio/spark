/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{
  NoSuchDatabaseException,
  NoSuchTableException
}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogTable,
  CatalogTablePartition
}
import org.apache.spark.util.ThreadUtils

import scala.util.Try

private[spark] class HiveExternalMemoryCatalog(conf: SparkConf,
                                               hadoopConf: Configuration)
    extends HiveExternalCatalog(conf, hadoopConf)
    with Logging {

  val meta_db = new mutable.HashMap[Int, CatalogDatabase]()
  val meta_tbl = new mutable.HashMap[Int, CatalogTable]()
  val meta_partition = new mutable.HashMap[Int, CatalogTablePartition]()
  val refreshTime =
    conf.get("spark.sql.hiveCatalog.memory.refreshTime", "600").toInt
  val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "hive-memory-catalog-refresh"
  )

  RefreshThreadStart()

  override def tableExists(db: String, table: String): Boolean = {
    try {
      getTable(db, table)
      true
    } catch {
      case e: NoSuchTableException => false
    }
  }

  override def getTable(db: String, table: String): CatalogTable = {
    meta_tbl.synchronized {
      meta_tbl.getOrElseUpdate(getTableKey(db, table), {
        super.getTable(db, table)
      })
    }
  }

  override def dropTable(db: String,
                         table: String,
                         ignoreIfNotExists: Boolean,
                         purge: Boolean): Unit = {

    meta_tbl.synchronized {
      meta_tbl.remove(getTableKey(db, table))
    }
    super.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override def databaseExists(db: String): Boolean = {
    try {
      getDatabase(db)
      true
    } catch {
      case e: NoSuchDatabaseException => false
    }
  }

  override def getDatabase(db: String): CatalogDatabase = {
    meta_db.synchronized {
      meta_db.getOrElseUpdate(getDbKey(db), {
        super.getDatabase(db)
      })
    }
  }

  override def getPartition(db: String,
                            table: String,
                            spec: TablePartitionSpec): CatalogTablePartition = {
    meta_partition.synchronized {
      val partitionKey =
        spec.toArray.sortBy(_._1).map(i => s"k:${i._1};v:${i._2}").mkString(",")
      meta_partition.getOrElseUpdate(getPartitionKey(db, table, spec), {
        super.getPartition(db, table, spec)
      })
    }
  }

  private def getDbKey(db: String): Int = {
    s"db:$db".hashCode
  }
  private def getTableKey(db: String, table: String): Int = {
    s"db:$db-tbl:$table".hashCode
  }
  private def getPartitionKey(db: String,
                              table: String,
                              spec: TablePartitionSpec): Int = {
    val partitionKey =
      spec.toArray.sortBy(_._1).map(i => s"k:${i._1};v:${i._2}").mkString(",")
    s"db:$db-tbl:$table-part:$partitionKey".hashCode
  }

  private def RefreshThreadStart(): Unit = {
    executor.schedule(new Runnable {
      override def run(): Unit = {
        meta_db.synchronized(meta_db.clear())
        meta_tbl.synchronized(meta_tbl.clear())
        meta_partition.synchronized(meta_partition.clear())
      }
    }, refreshTime, TimeUnit.SECONDS)
  }

}
