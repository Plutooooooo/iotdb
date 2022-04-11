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

public abstract class BigArray {

  /** Initial number of segments in array. */
  public static final int INITIAL_SEGMENTS = 1024;

  /** The shift is used to compute the segment index. It is the logarithm of the segment size. */
  public static final int SEGMENT_SHIFT = 10;

  /** Size of a single segment of a BigArray */
  public static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;

  /** This mask is used to compute the offset in a segment. */
  public static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

  protected int capacity;
  protected int segments;

  /**
   * Returns the segment associated with a given index.
   *
   * @param index index of the target
   * @return the associated segment.
   */
  public static int segment(long index) {
    return (int) (index >>> SEGMENT_SHIFT);
  }

  /**
   * Returns the offset associated with a given index.
   *
   * @param index index of the target
   * @return the associated offset (in the associated {@link #segment(long) segment}).
   */
  public static int offset(long index) {
    return (int) (index & SEGMENT_MASK);
  }
}
