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
package org.apache.iotdb.db.mpp.sql.planner;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.LogicalPlanner.PlanBuilder;
import org.apache.iotdb.db.mpp.sql.statement.Expression;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementNode;
import org.apache.iotdb.db.mpp.sql.statement.component.InPredicate;
import org.apache.iotdb.db.mpp.sql.statement.crud.SubqueryExpression;

public class SubqueryPlanner {
  private final Analysis analysis;
  public SubqueryPlanner(Analysis analysis) {
    this.analysis = analysis;
  }

  public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Expression expression, StatementNode node){
    return null;
  }

  private PlanBuilder handleInPredicate(PlanBuilder subPlan, InPredicate inPredicate, StatementNode node){
    subPlan = handleUncorrelatedSubqueries(subPlan, inPredicate.getValue(), node);
    checkState(inPredicate.getValueList() instanceof SubqueryExpression);
    SubqueryExpression valueListSubquery = (SubqueryExpression) inPredicate.getValueList();
    PlanBuilder subqueryPlan = createPlanBuilder(uncoercedValueListSubquery);

  }
  private PlanBuilder handleScalarSubquery(PlanBuilder subPlan, Expression expression, StatementNode node){
    return null;
  }
}
