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
package org.apache.iotdb.db.mpp.sql.statement.component;

import java.util.Objects;
import org.apache.iotdb.db.mpp.sql.statement.Expression;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

public class InPredicate
    extends Expression
{
  private final Expression value;
  private final Expression valueList;

  private InPredicate(Expression value, Expression valueList)
  {
    this.value = value;
    this.valueList = valueList;
  }

  public Expression getValue()
  {
    return value;
  }

  public Expression getValueList()
  {
    return valueList;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context)
  {
    return visitor.visitInPredicate(this, context);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InPredicate that = (InPredicate) o;
    return Objects.equals(value, that.value) &&
        Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value, valueList);
  }
}
