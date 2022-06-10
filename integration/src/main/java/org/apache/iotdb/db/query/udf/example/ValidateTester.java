/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;

public class ValidateTester implements UDTF {

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateRequiredAttribute("k")
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT32),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT64))
        .validateInputSeriesDataType(
            1,
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT32),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT64));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT32));
  }
}
