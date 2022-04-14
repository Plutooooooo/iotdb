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
package org.apache.iotdb.db.mpp.operator.subquery.common;

import org.apache.iotdb.db.mpp.operator.process.subquery.common.IntBuildSideHashSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntBuildSideHashSetTest {
  @Test
  public void testAddTsBlock() {
    TsBlock tsBlock = createTsBlockInOrder(100);
    IntBuildSideHashSet set = new IntBuildSideHashSet(0, 100);
    boolean success = set.addTsBlock(tsBlock);
    assertTrue(success);
  }

  @Test
  public void testContains1() {
    int positionCount = 1000000;
    TsBlock tsBlock = createTsBlockInOrder(positionCount);
    Column column = tsBlock.getColumn(0);
    IntBuildSideHashSet set = new IntBuildSideHashSet(0, 10);
    set.addTsBlock(tsBlock);
    for (int i = 0; i < positionCount; i++) {
      assertTrue(set.contains(i));
      assertTrue(set.contains(column, i));
    }
    System.out.println(set.getHashCollisions());
    for (int i = positionCount; i < 2 * positionCount; i++) {
      assertFalse(set.contains(i));
    }
  }

  @Test
  public void testContains2() {
    int positionCount = 10000;
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    int[] value = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      int random = (int) (Math.random() * (Integer.MAX_VALUE));
      value[i] = random;
      builder.getColumnBuilder(0).writeInt(random);
      builder.declarePosition();
    }

    TsBlock tsBlock = builder.build();
    Column column = tsBlock.getColumn(0);
    IntBuildSideHashSet set = new IntBuildSideHashSet(0, 10000);
    set.addTsBlock(tsBlock);
    for (int i = 0; i < positionCount; i++) {
      assertTrue(set.contains(value[i]));
      assertTrue(set.contains(column, i));
    }
    System.out.println("hashCollisions is : " + set.getHashCollisions());
  }

  private TsBlock createTsBlockInOrder(int positionCount) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (int i = 0; i < positionCount; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeInt(i);
      builder.declarePosition();
    }
    return builder.build();
  }
}
