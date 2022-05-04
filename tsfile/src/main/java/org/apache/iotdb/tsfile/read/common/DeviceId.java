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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeviceId implements Comparable<DeviceId> {

  private final String[] nodes;

  /** this field dose not need to be serialized. * */
  private String deviceIdString;

  public DeviceId(String deviceId) {
    nodes = splitDeviceIdToNodes(deviceId);
    deviceIdString = deviceId;
  }

  public DeviceId(String[] nodes) {
    this.nodes = nodes;
  }

  public static String[] splitDeviceIdToNodes(String deviceId) throws TsFileRuntimeException {
    if (deviceId.length() == 0) {
      throw new TsFileRuntimeException("Length of deviceId should be greater than 0.");
    }
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
          } else if (afterQuote == TsFileConstant.PATH_SEPARATOR_CHAR) {
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

  public int serializeTo(ByteBuffer byteBuffer) {
    int len = ReadWriteIOUtils.write(nodes.length, byteBuffer);
    for (String node : nodes) {
      len += ReadWriteIOUtils.write(node, byteBuffer);
    }
    return len;
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int len = ReadWriteIOUtils.write(nodes.length, outputStream);
    for (String node : nodes) {
      len += ReadWriteIOUtils.write(node, outputStream);
    }
    return len;
  }

  public static DeviceId deserializeFrom(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    String[] nodes = new String[size];
    for (int i = 0; i < size; i++) {
      nodes[i] = ReadWriteIOUtils.readString(byteBuffer);
    }
    return new DeviceId(nodes);
  }

  public static DeviceId deserializeFrom(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    String[] nodes = new String[size];
    for (int i = 0; i < size; i++) {
      nodes[i] = ReadWriteIOUtils.readString(inputStream);
    }
    return new DeviceId(nodes);
  }

  public static DeviceId deserializeFrom(TsFileInput input, long offset) throws IOException {
    long offsetVar = offset;
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    input.read(buffer, offsetVar);
    buffer.flip();
    int size = buffer.getInt();
    offsetVar += Integer.BYTES;
    String[] nodes = new String[size];
    for (int i = 0; i < size; i++) {
      String node = input.readVarIntString(offsetVar);
      int nodeLen = node.getBytes(TSFileConfig.STRING_CHARSET).length;
      offsetVar += (nodeLen + ReadWriteForEncodingUtils.varIntSize(nodeLen));
      nodes[i] = node;
    }
    return new DeviceId(nodes);
  }

  public int getSerializedSize() {
    int len = 0;
    len += ReadWriteForEncodingUtils.varIntSize(nodes.length);
    for (String node : nodes) {
      int nodeLen = node.getBytes(TSFileConfig.STRING_CHARSET).length;
      len += (nodeLen + ReadWriteForEncodingUtils.varIntSize(nodeLen));
    }
    return len;
  }

  public String getDeviceIdString() {
    if (deviceIdString == null) {
      StringBuilder s = new StringBuilder(parseNodeString(nodes[0]));
      for (int i = 1; i < nodes.length; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(parseNodeString(nodes[i]));
      }
      deviceIdString = s.toString();
    }
    return deviceIdString;
  }

  @Override
  public String toString() {
    return getDeviceIdString();
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

  /**
   * wrap node that has . or ` in it with ``
   *
   * @param node
   * @return
   */
  protected String parseNodeString(String node) {
    node = node.replace("`", "``");
    if (node.contains(TsFileConstant.BACK_QUOTE_STRING)
        || node.contains(TsFileConstant.PATH_SEPARATOR)) {
      node = String.format("`%s`", node);
    }
    return node;
  }

  @Override
  public int compareTo(DeviceId o) {
    return 0;
  }
}
