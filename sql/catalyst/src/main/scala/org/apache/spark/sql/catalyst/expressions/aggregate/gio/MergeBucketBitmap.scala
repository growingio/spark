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

import io.growing.bitmap.BucketBitMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

/**
 * Merge [[BucketBitMap]] in each row into one [[BucketBitMap]].
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(bucket_bitmap) - Merge `bucket_bitmap` in each row into one bucket bitmap.
    """
)
case class MergeBucketBitmap(
    bitmap: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[BucketBitMap] with ImplicitCastInputTypes {

  def this(bitmap: Expression) = {
    this(bitmap, 0, 0)
  }

  override def createAggregationBuffer(): BucketBitMap = new BucketBitMap()

  override def update(buffer: BucketBitMap, input: InternalRow): BucketBitMap = {
    val _bitmap = new BucketBitMap(bitmap.eval(input).asInstanceOf[Array[Byte]])
    buffer.or(_bitmap)
  }

  override def merge(buffer: BucketBitMap, input: BucketBitMap): BucketBitMap = {
    buffer.or(input)
  }

  override def eval(buffer: BucketBitMap): Any = {
    if (buffer.isEmpty) {
      null
    } else {
      buffer.getBytes
    }
  }

  override def serialize(buffer: BucketBitMap): Array[Byte] = {
    buffer.getBytes
  }

  override def deserialize(storageFormat: Array[Byte]): BucketBitMap = {
    new BucketBitMap(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(bitmap)

  override def prettyName: String = "merge_bucket_bitmap"
}

