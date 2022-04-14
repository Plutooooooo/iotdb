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
package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.db.mpp.sql.statement.Expression;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

import java.util.Objects;

public class SubqueryExpression extends Expression {

  private final QueryStatement queryStatement;

  public SubqueryExpression(QueryStatement queryStatement) {
    this.queryStatement = queryStatement;
  }

  public QueryStatement getQueryStatement() {
    return queryStatement;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SubqueryExpression that = (SubqueryExpression) o;
    return Objects.equals(queryStatement, that.getQueryStatement());
  }

  @Override
  public int hashCode() {
    return queryStatement.hashCode();
  }
}
