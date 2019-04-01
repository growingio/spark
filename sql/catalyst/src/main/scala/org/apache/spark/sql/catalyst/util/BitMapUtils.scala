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

package org.apache.spark.sql.catalyst.util

import java.{lang, util}

import io.growing.bitmap.{BucketBitMap, CBitMap, RoaringBitmap, SBitMap}
import scala.collection.JavaConverters._

object BitMapUtils {

  /**
   * merge rid (ruleId index) into bucketBitmap
   */
  def mergeRuleId(bitmap: BucketBitMap, rid: Short): BucketBitMap = {
    if (bitmap == null || rid < 0) {
      return bitmap
    }
    val newContainer = new util.HashMap[lang.Short, RoaringBitmap]()
    bitmap.getContainer.asScala.foreach { kv =>
      val newKey = mergeNewKey(rid, kv._1)
      newContainer.put(newKey.toShort, kv._2)
    }
    new BucketBitMap(newContainer, false)
  }

  /**
   * Generate newKey for SID/EID...
   */
  private def mergeNewKey(rid: Short, id: Short): Short = {
    // rate is 8:8
    val key = ((rid & 0x00FF) << 8) | (id & 0x00FF)
    key.shortValue()
  }

  /**
   * merge rid (ruleId index) into CBitmap
   */
  def mergeRuleId(cbitmap: CBitMap, rid: Short): CBitMap = {
    val cbm = cbitmap.getContainer
    cbm.keySet().asScala.foreach { pos =>
      cbm.put(pos, mergeRuleId(cbm.get(pos), rid))
    }
    cbitmap
  }

  /**
   * merge rid (ruleId index) into SBitmap, merge into bucket id
   */
  def mergeRuleIdIntoBucketId(sbitmap: SBitMap, rid: Short): SBitMap = {
    val sbm = sbitmap.getContainer
    sbm.keySet().asScala.foreach { pos =>
      sbm.put(pos, mergeRuleId(sbm.get(pos), rid))
    }
    sbitmap
  }

  /**
   * merge rid (ruleId index) into SBitmap, merge into session id
   */
  def mergeRuleIdIntoSessionId(sbitmap: SBitMap, rid: Short): SBitMap = {
    val newContainer = new util.HashMap[lang.Integer, BucketBitMap]()
    sbitmap.getContainer.asScala.foreach { kv =>
      val newKey = mergeNewKey(rid, kv._1.shortValue())
      newContainer.put(newKey.intValue(), kv._2)
    }
    new SBitMap(newContainer, false)
  }
}
