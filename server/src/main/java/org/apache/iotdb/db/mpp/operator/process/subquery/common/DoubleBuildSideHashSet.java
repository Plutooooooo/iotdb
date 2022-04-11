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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import static java.lang.Math.toIntExact;

public class DoubleBuildSideHashSet extends BuildSideHashSet {

  private DoubleBigArray values;
  private IntBigArray elementIds;
  private DoubleBigArray valuesByElementId;

  /**
   * @param hashChannel index of the value column used to build hashset
   * @param estimatedElementCount estimated count of elements that will be stored.
   */
  public DoubleBuildSideHashSet(int hashChannel, int estimatedElementCount) {
    super(hashChannel, estimatedElementCount);

    values = new DoubleBigArray();
    values.ensureCapacity(hashCapacity);
    elementIds = new IntBigArray(-1);
    elementIds.ensureCapacity(hashCapacity);
    valuesByElementId = new DoubleBigArray();
    valuesByElementId.ensureCapacity(maxElementCount);

    // todo: memory control
  }

  @Override
  public long getEstimatedSize() {
    return 0;
  }

  @Override
  public boolean contains(Column column, int position) {
    if (column.isNull(position)) {
      return nullElementId >= 0;
    }
    double value = column.getDouble(position);
    int hashPosition = getHashPosition(value, mask);

    // check whether hashset contains the value using linear conflict resolution
    while (true) {
      int elementId = elementIds.get(hashPosition);
      if (elementId == -1) {
        return false;
      }
      if (value == values.get(hashPosition)) {
        return true;
      }
      hashPosition = (hashPosition + 1) & mask;
    }
  }

  @Override
  protected int putIfAbsent(Column column, int position) {
    if (column.isNull(position)) {
      if (nullElementId < 0) {
        // set null element id
        nullElementId = nextElementId++;
      }
      return nullElementId;
    }

    double value = column.getDouble(position);
    int hashPosition = getHashPosition(value, mask);

    // look for an empty slot or a slot containing this key
    while (true) {
      int elementId = elementIds.get(hashPosition);
      if (elementId == -1) {
        break;
      }

      if (value == values.get(hashPosition)) {
        return elementId;
      }

      // linear conflict resolution
      hashPosition = (hashPosition + 1) & mask;
      hashCollisions++;
    }
    return addNewElement(hashPosition, value);
  }

  private int addNewElement(int hashPosition, double value) {
    // record element id
    int elementId = nextElementId++;

    values.set(hashPosition, value);
    valuesByElementId.set(elementId, value);
    elementIds.set(hashPosition, elementId);

    // increase capacity, if necessary
    if (needRehash()) {
      tryRehash();
    }
    return elementId;
  }

  @Override
  protected boolean tryRehash() {
    long newCapacityLong = hashCapacity * 2L;
    if (newCapacityLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Size of BigArray cannot exceed 2147483647.");
    }
    int newCapacity = toIntExact(newCapacityLong);

    // todo: check memory

    int newMask = newCapacity - 1;
    DoubleBigArray newValues = new DoubleBigArray();
    newValues.ensureCapacity(newCapacity);
    IntBigArray newElementIds = new IntBigArray(-1);
    newElementIds.ensureCapacity(newCapacity);

    for (int elementId = 0; elementId < nextElementId; elementId++) {
      if (elementId == nullElementId) {
        continue;
      }
      double value = valuesByElementId.get(elementId);

      // find an empty slot for the address
      int hashPosition = getHashPosition(value, newMask);
      while (newElementIds.get(hashPosition) != -1) {
        hashPosition = (hashPosition + 1) & newMask;
        hashCollisions++;
      }

      // record the mapping
      newValues.set(hashPosition, value);
      newElementIds.set(hashPosition, elementId);
    }

    mask = newMask;
    hashCapacity = newCapacity;
    maxElementCount = calculateMaxElementCount(hashCapacity);
    values = newValues;
    elementIds = newElementIds;

    valuesByElementId.ensureCapacity(maxElementCount);
    return true;
  }

  private int getHashPosition(double value, int mask) {
    return Double.hashCode(value) & mask;
  }

  @TestOnly
  public boolean contains(double value) {
    int hashPosition = getHashPosition(value, mask);

    // check whether hashset contains the value using linear conflict resolution
    while (true) {
      int elementId = elementIds.get(hashPosition);
      if (elementId == -1) {
        return false;
      }
      if (value == values.get(hashPosition)) {
        return true;
      }
      hashPosition = (hashPosition + 1) & mask;
    }
  }
}
