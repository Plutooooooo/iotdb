/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.operator.process.subquery.common;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * java.util.HashSet can not accept type argument of primitive type. This is a simplified version of
 * java.util.HashSet that can accept primitive type as type argument.
 */
public abstract class BuildSideHashSet {

  public static final float LOAD_FACTOR = 0.75f;

  /** length of the big array */
  protected int hashCapacity;

  /** max count of elements in the hashset */
  protected int maxElementCount;

  /** elementId for the null value */
  protected int nullElementId = -1;

  protected int hashChannel;
  protected int mask;

  protected int nextElementId;
  protected long hashCollisions;

  protected BuildSideHashSet(int hashChannel, int estimatedElementCount) {
    checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
    checkArgument(estimatedElementCount > 0, "expectedElementCount must be greater than zero");

    this.hashChannel = hashChannel;

    hashCapacity = HashUtils.calculateHashCapacity(estimatedElementCount, LOAD_FACTOR);

    maxElementCount = calculateMaxElementCount(hashCapacity);
    mask = hashCapacity - 1;
  }

  /**
   * Returns estimated memory usage in bytes.
   *
   * @return estimated memory usage in bytes
   */
  public abstract long getEstimatedSize();

  /**
   * Returns true if hashset contains the element of tsBlock at the specified position.
   *
   * @param column input Column
   * @param position row index for target element
   * @return true if hashset contains the element of column at the specified position
   */
  public abstract boolean contains(Column column, int position);

  /**
   * Try to rehash.
   *
   * @return true if rehash successfully
   */
  protected abstract boolean tryRehash();

  /**
   * Put value of specific position into hashset.
   *
   * @param column input column
   * @param position row index of the value
   * @return element id of the value
   */
  protected abstract int putIfAbsent(Column column, int position);

  /**
   * Returns the count of collisions during building the hashset.
   *
   * @return the count of collisions during building the hashset
   */
  public long getHashCollisions() {
    return hashCollisions;
  }

  /**
   * Returns true if tsBlock is successfully added.
   *
   * @param tsBlock input TsBlock
   * @return true if tsBlock is successfully added
   */
  public boolean addTsBlock(TsBlock tsBlock) {
    if (needRehash() && !tryRehash()) {
      return false;
    }
    int positionCount = tsBlock.getPositionCount();
    Column column = tsBlock.getColumn(hashChannel);
    int cursor = 0;

    // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory
    // to do so.
    // Therefore needRehash() will not return true unless we have failed to rehash.
    while (cursor < positionCount && !needRehash()) {
      // get the group for the current row
      putIfAbsent(column, cursor);
      cursor++;
    }
    return cursor == positionCount;
  }

  /**
   * Returns the max count of elements with given hashCapacity.
   *
   * @param hashCapacity capacity of the hashset
   * @return the max count of elements with given hashCapacity
   */
  protected int calculateMaxElementCount(int hashCapacity) {
    checkArgument(hashCapacity > 0, "hashCapacity must be greater than 0");
    int maxElementCount = (int) Math.ceil(hashCapacity * LOAD_FACTOR);
    if (maxElementCount == hashCapacity) {
      maxElementCount--;
    }
    checkArgument(hashCapacity > maxElementCount, "hashCapacity must be larger than maxFill");
    return maxElementCount;
  }

  protected boolean needRehash() {
    return nextElementId >= maxElementCount;
  }
}
