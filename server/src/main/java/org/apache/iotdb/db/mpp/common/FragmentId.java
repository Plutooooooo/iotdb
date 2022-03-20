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
package org.apache.iotdb.db.mpp.common;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FragmentId {

  private final QueryId queryId;
  private final int id;

  public static FragmentId valueOf(String stageId) {
    List<String> ids = QueryId.parseDottedId(stageId, 2, "stageId");
    return valueOf(ids);
  }

  public static FragmentId valueOf(List<String> ids) {
    checkArgument(ids.size() == 2, "Expected two ids but got: %s", ids);
    return new FragmentId(new QueryId(ids.get(0)), Integer.parseInt(ids.get(1)));
  }

  public FragmentId(String queryId, int id) {
    this(new QueryId(queryId), id);
  }

  public FragmentId(QueryId queryId, int id) {
    this.queryId = requireNonNull(queryId, "queryId is null");
    this.id = id;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getId() {
    return id;
  }
}
