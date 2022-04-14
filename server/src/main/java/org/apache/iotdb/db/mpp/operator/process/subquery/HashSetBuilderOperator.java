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
package org.apache.iotdb.db.mpp.operator.process.subquery;

import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.BuildSideHashSet;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.DoubleBuildSideHashSet;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.FloatBuildSideHashSet;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.IntBuildSideHashSet;
import org.apache.iotdb.db.mpp.operator.process.subquery.common.LongBuildSideHashSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class HashSetBuilderOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private Operator buildSource;
  private int hashChannel;
  private TSDataType sourceType;
  private BuildSideHashSet buildSideHashSet;
  private static int ESTIMATED_COUNT = 10000;

  public HashSetBuilderOperator(
      OperatorContext operatorContext,
      Operator buildSource,
      int hashChannel,
      TSDataType sourceType) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.buildSource = requireNonNull(buildSource, "buildSource is null");
    this.hashChannel = hashChannel;
    this.sourceType = sourceType;
    this.buildSideHashSet = createBuildSideHashSet(sourceType, hashChannel, ESTIMATED_COUNT);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public boolean isFinished() throws IOException {
    return false;
  }

  public BuildSideHashSet getBuildSideHashSet() {
    return buildSideHashSet;
  }

  private BuildSideHashSet createBuildSideHashSet(
      TSDataType tsDataType, int hashChannel, int estimatedElementCount) {
    switch (tsDataType) {
      case INT32:
        return new IntBuildSideHashSet(hashChannel, estimatedElementCount);
      case INT64:
        return new LongBuildSideHashSet(hashChannel, estimatedElementCount);
      case FLOAT:
        return new FloatBuildSideHashSet(hashChannel, estimatedElementCount);
      case DOUBLE:
        return new DoubleBuildSideHashSet(hashChannel, estimatedElementCount);
      default:
        throw new IllegalArgumentException("illegal type");
    }
  }
}
