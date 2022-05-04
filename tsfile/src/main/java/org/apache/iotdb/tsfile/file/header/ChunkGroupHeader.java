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

package org.apache.iotdb.tsfile.file.header;

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.read.common.DeviceId;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ChunkGroupHeader {

  private static final byte MARKER = MetaMarker.CHUNK_GROUP_HEADER;

  private final DeviceId deviceId;

  // this field does not need to be serialized.
  private int serializedSize;

  /**
   * constructor of CHUNK_GROUP_HEADER.
   *
   * @param deviceId device ID
   */
  public ChunkGroupHeader(DeviceId deviceId) {
    this.deviceId = deviceId;
    this.serializedSize = getSerializedSize(deviceId);
  }

  private int getSerializedSize(DeviceId deviceId) {
    return Byte.BYTES + deviceId.getSerializedSize();
  }

  /**
   * deserialize from inputStream.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_HEADER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(InputStream inputStream, boolean markerRead)
      throws IOException {
    if (!markerRead) {
      byte marker = (byte) inputStream.read();
      if (marker != MARKER) {
        MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    DeviceId deviceId = DeviceId.deserializeFrom(inputStream);
    return new ChunkGroupHeader(deviceId);
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param markerRead Whether the marker of the CHUNK_GROUP_HEADER is read ahead.
   */
  public static ChunkGroupHeader deserializeFrom(TsFileInput input, long offset, boolean markerRead)
      throws IOException {
    long offsetVar = offset;
    if (!markerRead) {
      offsetVar++;
    }
    DeviceId deviceId = DeviceId.deserializeFrom(input, offsetVar);
    return new ChunkGroupHeader(deviceId);
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public DeviceId getDeviceId() {
    return deviceId;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream output stream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int length = 0;
    length += ReadWriteIOUtils.write(MARKER, outputStream);
    length += deviceId.serializeTo(outputStream);
    return length;
  }

  @Override
  public String toString() {
    return "ChunkGroupHeader{"
        + "deviceID='"
        + deviceId.getDeviceIdString()
        + '\''
        + ", serializedSize="
        + serializedSize
        + '}';
  }
}
