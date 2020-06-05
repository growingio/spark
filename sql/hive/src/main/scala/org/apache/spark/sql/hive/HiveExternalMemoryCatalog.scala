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
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogTable,
  CatalogTablePartition
}
import org.apache.spark.util.ThreadUtils

private[spark] class HiveExternalMemoryCatalog(conf: SparkConf,
                                               hadoopConf: Configuration)
    extends HiveExternalCatalog(conf, hadoopConf)
    with Logging {

  val meta_db = new mutable.HashMap[String, CatalogDatabase]()
  val meta_tbl = new mutable.HashMap[String, CatalogTable]()
  val refreshTime =
    conf.get("spark.sql.hiveCatalog.memory.refreshTime", "600").toInt
  val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "hive-memory-catalog-refresh"
  )

  RefreshThreadStart()

  override def getTable(db: String, table: String): CatalogTable = {
    meta_tbl.synchronized {
      meta_tbl.getOrElseUpdate(s"db:$db-tbl:$table", {
        super.getTable(db, table)
      })
    }
  }

  override def dropTable(db: String,
                         table: String,
                         ignoreIfNotExists: Boolean,
                         purge: Boolean): Unit = {

    meta_tbl.synchronized {
      meta_tbl.remove(s"db:$db-tbl:$table")
    }
    super.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    meta_db.synchronized {
      meta_db.getOrElseUpdate(s"db:$db:$db", {
        super.getDatabase(db)
      })
    }
  }

  override def getPartition(db: String,
                            table: String,
                            spec: TablePartitionSpec): CatalogTablePartition =
    super.getPartition(db, table, spec)

  private def RefreshThreadStart(): Unit = {
    executor.schedule(new Runnable {
      override def run(): Unit = {
        meta_db.synchronized(meta_db.clear())
        meta_tbl.synchronized(meta_tbl.clear())
      }
    }, refreshTime, TimeUnit.SECONDS)
  }

}
