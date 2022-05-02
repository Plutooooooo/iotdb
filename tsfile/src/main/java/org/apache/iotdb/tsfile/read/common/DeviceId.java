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
package org.apache.iotdb.tsfile.read.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class DeviceId {

  private String[] nodes;

  public DeviceId(String deviceId) {}

  public DeviceId(String[] nodes) {}


  public static String[] splitPathToDetachedPath(String deviceId) throws TsFileRuntimeException {
    // NodeName is treated as identifier. When parsing identifier, unescapeJava is called.
    // Therefore we call unescapeJava here.
    deviceId = StringEscapeUtils.unescapeJava(deviceId);
    if (deviceId.endsWith(TsFileConstant.PATH_SEPARATOR)) {
      throw new TsFileRuntimeException(deviceId);
    }
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    int endIndex;
    int length = deviceId.length();
    for (int i = 0; i < length; i++) {
      if (deviceId.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        String node = deviceId.substring(startIndex, i);
        if (node.isEmpty()) {
          throw new TsFileRuntimeException(deviceId);
        }
        nodes.add(node);
        startIndex = i + 1;
      } else if (deviceId.charAt(i) == TsFileConstant.BACK_QUOTE) {
        startIndex = i + 1;
        endIndex = deviceId.indexOf(TsFileConstant.BACK_QUOTE, startIndex);
        if (endIndex == -1) {
          // single '`', like root.sg.`s
          throw new TsFileRuntimeException(deviceId);
        }
        while (endIndex != -1 && endIndex != length - 1) {
          char afterQuote = deviceId.charAt(endIndex + 1);
          if (afterQuote == TsFileConstant.BACK_QUOTE) {
            // for example, root.sg.```
            if (endIndex == length - 2) {
              throw new TsFileRuntimeException(deviceId);
            }
            endIndex = deviceId.indexOf(TsFileConstant.BACK_QUOTE, endIndex + 2);
          } else if (afterQuote == '.') {
            break;
          } else {
            throw new TsFileRuntimeException(deviceId);
          }
        }
        // replace `` with ` in a quoted identifier
        String node = deviceId.substring(startIndex, endIndex).replace("``", "`");
        if (node.isEmpty()) {
          throw new TsFileRuntimeException(deviceId);
        }

        nodes.add(node);
        // skip the '.' after '`'
        i = endIndex + 1;
        startIndex = endIndex + 2;
      }
    }
    // last node
    if (startIndex <= deviceId.length() - 1) {
      String node = deviceId.substring(startIndex);
      if (node.isEmpty()) {
        throw new TsFileRuntimeException(deviceId);
      }
      nodes.add(node);
    }
    return nodes.toArray(new String[0]);
  }

  public String[] getNodes() {
    return this.nodes;
  }

  public void serializeTo(ByteBuffer byteBuffer){
    ReadWriteIOUtils.write(nodes.length,byteBuffer);
    for(String node : nodes){
      ReadWriteIOUtils.write(node,byteBuffer);
    }
  }

  public static DeviceId deserializeFrom(ByteBuffer byteBuffer){
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    String[] nodes = new String[size];
    for(int i=0;i<size;i++){
      nodes[i] = ReadWriteIOUtils.readString(byteBuffer);
    }
    return new DeviceId(nodes);
  }

  public static DeviceId deserializeFrom(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    String[] nodes = new String[size];
    for(int i=0;i<size;i++){
      nodes[i] = ReadWriteIOUtils.readString(inputStream);
    }
    return new DeviceId(nodes);
  }

  public static DeviceId deserializeFrom(TsFileInput input, long offset)
      throws IOException {
    long offsetVar = offset;
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    input.read(buffer, offsetVar);
    buffer.flip();
    int size = buffer.getInt();
    offsetVar += Integer.BYTES;
    String deviceID = input.readVarIntString(offsetVar);
    return new ChunkGroupHeader(deviceID);
  }

  public int getSerializedSize(){
    int len = 0;
    len += ReadWriteForEncodingUtils.varIntSize(nodes.length);
    for(String node:nodes){
      int nodeLen = node.getBytes(TSFileConfig.STRING_CHARSET).length;
      len += (nodeLen + ReadWriteForEncodingUtils.varIntSize(nodeLen));
    }
    return len;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeviceId that = (DeviceId) o;
    return Arrays.equals(that.getNodes(), this.nodes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(nodes);
  }
}
