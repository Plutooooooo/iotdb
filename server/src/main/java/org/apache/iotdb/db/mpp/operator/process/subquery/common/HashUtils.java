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

public class HashUtils {

  public static final long murmurHash3(long x) {
    x ^= x >>> 33;
    x *= -49064778989728563L;
    x ^= x >>> 33;
    x *= -4265267296055464877L;
    x ^= x >>> 33;
    return x;
  }

  public static long nextPowerOfTwo(long x) {
    if (x == 0L) {
      return 1L;
    } else {
      --x;
      x |= x >> 1;
      x |= x >> 2;
      x |= x >> 4;
      x |= x >> 8;
      x |= x >> 16;
      return (x | x >> 32) + 1L;
    }
  }

  public static int maxFill(int n, float f) {
    return (int) Math.ceil((double) ((float) n * f));
  }

  public static long maxFill(long n, float f) {
    return (long) Math.ceil((double) ((float) n * f));
  }

  /**
   * Returns the needed length of BigArray and length = 2^n.
   *
   * @param estimatedElementCount estimated count of elements
   * @param f load factor
   * @return the needed length of BigArray
   */
  public static int calculateHashCapacity(int estimatedElementCount, float f) {
    long s = nextPowerOfTwo((long) Math.ceil(((float) estimatedElementCount / f)));
    if (s > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Too large (" + estimatedElementCount + " expected elements with load factor " + f + ")");
    } else {
      return (int) s;
    }
  }
}
