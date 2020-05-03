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
package org.apache.iotdb.db.index.algorithm;

import static org.apache.iotdb.db.index.TestUtils.deserializeIndexChunk;
import static org.apache.iotdb.db.index.common.IndexConstant.DISTANCE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE;
import static org.apache.iotdb.db.index.common.IndexConstant.ELB_TYPE_ELE;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.L_INFINITY;
import static org.apache.iotdb.db.index.common.IndexType.ELB;
import static org.apache.iotdb.db.index.common.IndexType.PAA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.apache.iotdb.db.index.TestUtils;
import org.apache.iotdb.db.index.TestUtils.Validation;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexChunkMeta;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ELBAndPAAIndexTest {

  private static final String storageGroup = "root.v";
  private static final String p1 = "root.v.p1";
  private static final String p2 = "root.v.p2";
  private static final String tempIndexFileDir = "index/root.v/";
  private static final String tempIndexFileName = "index/root.v/demo_elb_paa_index";

  private void prepareMManager() throws MetadataException {
    MManager mManager = MManager.getInstance();
    mManager.init();
    mManager.setStorageGroup(storageGroup);
    mManager.createTimeseries(p1, TSDataType.INT32, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    mManager.createTimeseries(p2, TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    Map<String, String> elbProps = new HashMap<>();
    elbProps.put(INDEX_WINDOW_RANGE, "10");
    elbProps.put(INDEX_SLIDE_STEP, "10");
    elbProps.put(DISTANCE, L_INFINITY);
    elbProps.put(ELB_TYPE, ELB_TYPE_ELE);
    Map<String, String> paaProps = new HashMap<>();
    paaProps.put(INDEX_WINDOW_RANGE, "10");
    paaProps.put(INDEX_SLIDE_STEP, "10");

    mManager.createIndex(Collections.singletonList(p1), new IndexInfo(ELB, 0, elbProps));
    mManager.createIndex(Collections.singletonList(p1), new IndexInfo(PAA, 0, paaProps));
    mManager.createIndex(Collections.singletonList(p2), new IndexInfo(ELB, 0, elbProps));
  }

  @Before
  public void setUp() throws Exception {
    MManager.getInstance().init();
    MManager.getInstance().clear();
    EnvironmentUtils.envSetUp();
    TestUtils.clearIndexFile(tempIndexFileName);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    TestUtils.clearIndexFile(tempIndexFileName);
  }


  @Test
  public void testMultiThreadWrite()
      throws MetadataException, ExecutionException, InterruptedException, IOException {
    prepareMManager();
    IoTDBDescriptor.getInstance().getConfig().setIndexBufferSize(500);
    FSFactoryProducer.getFSFactory().getFile(tempIndexFileDir).mkdirs();
    // Prepare data
    TVList p1List = TVListAllocator.getInstance().allocate(TSDataType.INT32);
    TVList p2List = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
    for (int i = 0; i < 100; i++) {
      p1List.putInt(i * 2, i * 2);
      p2List.putFloat(i * 3, i * 3);
    }

    String gtStrP1ELB = ""
        + "(0,[0-18,10])(1,[20-38,10])(2,[40-58,10])(3,[60-78,10])"
        + "(4,[80-98,10])(5,[100-118,10])(6,[120-138,10])(7,[140-158,10])"
        + "(8,[160-178,10])(9,[180-198,10])";
    String gtStrP1PAA = ""
        + "(0,[0-10,4])(1,[10-20,4])(2,[20-30,4])(3,[30-40,4])(4,[40-50,4])(5,[50-60,4])"
        + "(6,[60-70,4])(7,[70-80,4])(8,[80-90,4])(9,[90-100,4])(10,[100-110,4])(11,[110-120,4])"
        + "(12,[120-130,4])(13,[130-140,4])(14,[140-150,4])(15,[150-160,4])(16,[160-170,4])"
        + "(17,[170-180,4])(18,[180-190,4])";
    String gtStrP2ELB = ""
        + "(0,[0-27,10])(1,[30-57,10])(2,[60-87,10])(3,[90-117,10])"
        + "(4,[120-147,10])(5,[150-177,10])(6,[180-207,10])(7,[210-237,10])"
        + "(8,[240-267,10])(9,[270-297,10])";
    List<Pair<IndexType, String>> gtP1 = new ArrayList<>();
    gtP1.add(new Pair<>(ELB, gtStrP1ELB));
    gtP1.add(new Pair<>(PAA, gtStrP1PAA));
    List<Pair<IndexType, String>> gtP2 = new ArrayList<>();
    gtP2.add(new Pair<>(ELB, gtStrP2ELB));

    List<Validation> tasks = new ArrayList<>();
    tasks.add(new Validation(p1, p1List, gtP1));
    tasks.add(new Validation(p2, p2List, gtP2));
    // check result
    checkIndexFlushAndResult(tasks, storageGroup, tempIndexFileDir, tempIndexFileName);
  }

  private static void checkIndexFlushAndResult(List<Validation> tasks, String storageGroup,
      String indexFileDir, String indexFileName)
      throws ExecutionException, InterruptedException, IOException {
    IndexFileProcessor indexFileProcessor = new IndexFileProcessor(storageGroup, indexFileDir,
        indexFileName, true);

    indexFileProcessor.startFlushMemTable();
    for (Validation task : tasks) {
      indexFileProcessor.buildIndexForOneSeries(new Path(task.path), task.tvList);
    }
    indexFileProcessor.endFlushMemTable();
    Assert.assertEquals(0, indexFileProcessor.getMemoryUsed().get());
    Assert.assertEquals(0, indexFileProcessor.getNumIndexBuildTasks().get());
    indexFileProcessor.close();
    //read and check
    IndexIOReader reader = new IndexIOReader(indexFileName, false);
    for (Validation task : tasks) {
      for (Pair<IndexType, String> pair : task.gt) {
        IndexType indexType = pair.left;
        System.out.println(String.format("path: %s, index: %s", task.path, indexType));
        String gtDataStr = pair.right;
        List<IndexChunkMeta> metaChunkList = reader.getChunkMetas(task.path, pair.left);

        StringBuilder readStr = new StringBuilder();
        for (IndexChunkMeta chunkMeta : metaChunkList) {
          // data
          ByteBuffer readData = reader.getDataByChunkMeta(chunkMeta);
          readStr.append(deserializeIndexChunk(indexType, readData));
        }
        System.out.println(readStr.toString());
        Assert.assertEquals(gtDataStr, readStr.toString());
      }
    }
  }

}
