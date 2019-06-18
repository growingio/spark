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

package org.apache.spark.sql.catalyst.expressions.aggregate.gio

import io.growing.bitmap.CBitMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

/**
 * Collect bucket, uid and count into a [[CBitMap]].
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(bucket, uid, count) - Collect `bucket`, `uid` and `count` into a cbitmap.
    """
)
case class CollectCBitmap(
    bucket: Expression,
    uid: Expression,
    count: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[CBitMap] with ImplicitCastInputTypes {

  def this(bucket: Expression, uid: Expression, count: Expression) = {
    this(bucket, uid, count, 0, 0)
  }

  override def createAggregationBuffer(): CBitMap = new CBitMap()

  override def update(buffer: CBitMap, input: InternalRow): CBitMap = {
    val _bucket = bucket.eval(input).asInstanceOf[Int].toShort
    val _uid = uid.eval(input).asInstanceOf[Int]
    val _count = count.eval(input).asInstanceOf[Number].longValue()
    if (_uid >= 0) buffer.add(_bucket, _uid, _count)
    buffer
  }

  override def merge(buffer: CBitMap, input: CBitMap): CBitMap = {
    buffer.or(input)
  }

  override def eval(buffer: CBitMap): Any = {
    if (buffer.isEmpty) {
      null
    } else {
      buffer.getBytes
    }
  }

  override def serialize(buffer: CBitMap): Array[Byte] = {
    buffer.getBytes
  }

  override def deserialize(storageFormat: Array[Byte]): CBitMap = {
    new CBitMap(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, IntegerType, LongType)

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(bucket, uid, count)

  override def prettyName: String = "collect_cbitmap"
}
