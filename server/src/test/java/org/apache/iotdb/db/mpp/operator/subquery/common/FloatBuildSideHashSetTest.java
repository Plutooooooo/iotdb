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

import java.util.Collections;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.FloatBuildSideHashSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.junit.Test;

import static org.junit.Assert.*;

public class FloatBuildSideHashSetTest {

  @Test
  public void testAddTsBlock(){
    TsBlock tsBlock = createTsBlockInOrder(100);
    FloatBuildSideHashSet set = new FloatBuildSideHashSet(0,100);
    boolean success = set.addTsBlock(tsBlock);
    assertTrue(success);
  }

  @Test
  public void testContains(){
    int positionCount = 10000;
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));
    float[] value = new float[positionCount];
    for (int i = 0; i < positionCount; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      float random = (float) (Math.random()*(Integer.MAX_VALUE));
      value[i] = random;
      builder.getColumnBuilder(0).writeFloat(random);
      builder.declarePosition();
    }

    TsBlock tsBlock = builder.build();
    Column column = tsBlock.getColumn(0);
    FloatBuildSideHashSet set = new FloatBuildSideHashSet(0,10000);
    set.addTsBlock(tsBlock);
    for(int i = 0;i< positionCount;i++){
      assertTrue(set.contains(value[i]));
      assertTrue(set.contains(column,i));
    }
    System.out.println("hashCollisions is : "+set.getHashCollisions());
  }

  private TsBlock createTsBlockInOrder(int positionCount){
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));
    for (int i = 0; i < positionCount; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeFloat(i);
      builder.declarePosition();
    }
    return builder.build();
  }

}
