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

import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOfFloatArray;

public class FloatBigArray extends BigArray {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(FloatBigArray.class).instanceSize();
  private static final long SIZE_OF_SEGMENT = sizeOfFloatArray(SEGMENT_SIZE);

  private final float initialValue;

  private float[][] array;

  /** Creates a new big array containing one initial segment. */
  public FloatBigArray() {
    this(0f);
  }

  /**
   * Creates a new big array containing one initial segment filled with the initialValue.
   *
   * @param initialValue initialValue of the array
   */
  public FloatBigArray(float initialValue) {
    this.initialValue = initialValue;
    array = new float[INITIAL_SEGMENTS][];
    allocateNewSegment();
  }

  public float get(long index) {
    checkArgument(index < capacity && index >= 0, "Index out of bounds in FloatBigArray.");
    return array[segment(index)][offset(index)];
  }

  public void set(long index, float value) {
    checkArgument(index < capacity && index >= 0, "Index out of bounds in FloatBigArray.");
    array[segment(index)][offset(index)] = value;
  }

  /**
   * Returns the size of this big array in bytes.
   *
   * @return the size of this big array in bytes
   */
  public long sizeOf() {
    return INSTANCE_SIZE + SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
  }

  /**
   * Ensures this big array is at least the specified length. If the array is smaller, segments are
   * added until the array is larger then the specified length.
   */
  public void ensureCapacity(long length) {
    if (capacity > length) {
      return;
    }

    grow(length);
  }

  private void grow(long length) {
    int requiredSegments = segment(length) + 1;

    if (array.length < requiredSegments) {
      array = Arrays.copyOf(array, requiredSegments);
    }

    // add new segments
    while (segments < requiredSegments) {
      allocateNewSegment();
    }
  }

  private void allocateNewSegment() {
    float[] newSegment = new float[SEGMENT_SIZE];
    if (initialValue != 0) {
      Arrays.fill(newSegment, initialValue);
    }
    array[segments] = newSegment;
    capacity += SEGMENT_SIZE;
    segments++;
  }
}
