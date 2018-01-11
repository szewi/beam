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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;

/**
 * Contains helper methods for file based IO Integration tests.
 */
public class FileBasedIOITHelper {

  private FileBasedIOITHelper() {
  }

  public static IOTestPipelineOptions readTestPipelineOptions() {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline
        .testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    return PipelineOptionsValidator.validate(IOTestPipelineOptions.class, options);
  }

  public static String appendTimestampToPrefix(String filenamePrefix) {
    return String.format("%s_%s", filenamePrefix, new Date().getTime());
  }

  public static String appendHdfsNameNode(String filenamePrefix, String namenode){
    String hdfsPrefix = String.format("hdfs://%s:9000", namenode);
    if (!filenamePrefix.startsWith(hdfsPrefix)){
      return String.format("%s/%s", hdfsPrefix, filenamePrefix);
    } else {
      return filenamePrefix;
    }
  }

  public static List<Configuration> createHdfsConfiguration(String namenode){
    List<Configuration> hdfsParams = Lists.newArrayList();
    Configuration conf = new Configuration(true);
    String coreSite = String.format("<configuration>\n" + "    <property>\n"
        + "        <name>fs.defaultFS</name>\n"
        + "        <value>hdfs://%s:9000</value>\n" + "    </property>\n"
        + "</configuration>", namenode);

    String hdfsSite = String.format("<configuration>\n" + "    <property>\n"
        + "        <name>dfs.replication</name>\n" + "        <value>1</value>\n"
        + "    </property>\n" + "    <property>\n"
        + "        <name>dfs.client.use.datanode.hostname</name>\n"
        + "        <value>true</value>\n" + "    </property>\n" + "</configuration>");

    conf.addResource(coreSite);
    conf.addResource(hdfsSite);
    hdfsParams.add(conf);
    return hdfsParams;
  }

  public static String getExpectedHashForLineCount(Long lineCount) {
    Map<Long, String> expectedHashes = ImmutableMap.of(
        100_000L, "4c8bb3b99dcc59459b20fefba400d446",
        1_000_000L, "9796db06e7a7960f974d5a91164afff1",
        100_000_000L, "6ce05f456e2fdc846ded2abd0ec1de95"
    );

    String hash = expectedHashes.get(lineCount);
    if (hash == null) {
      throw new UnsupportedOperationException(
          String.format("No hash for that line count: %s", lineCount)
      );
    }
    return hash;
  }

  /**
   * Constructs text lines in files used for testing.
   */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  /**
   * Deletes matching files using the FileSystems API.
   */
  public static class DeleteFileFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match = Iterables
          .getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Set<ResourceId> resourceIds = new HashSet<>();
      for (MatchResult.Metadata metadataElem : match.metadata()) {
        resourceIds.add(metadataElem.resourceId());
      }

      FileSystems.delete(resourceIds);
    }
  }
}
