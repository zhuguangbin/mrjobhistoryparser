/**
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

package com.mediav.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobHistory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * This class is to view job history files.
 */
class MVHistoryViewer {
  private static SimpleDateFormat dateFormat = new SimpleDateFormat(
          "d-MMM-yyyy HH:mm:ss");
  private FileSystem fs;
  private Configuration conf;
  private Path historyLogDir;


  private PathFilter jobLogFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      return !(path.getName().endsWith(".xml"));
    }
  };

  public MVHistoryViewer(String outputDir, Configuration conf)
          throws IOException {
    this.conf = conf;
    historyLogDir = new Path(outputDir);
    try {
      fs = historyLogDir.getFileSystem(this.conf);
      if (!fs.exists(historyLogDir)) {
        throw new IOException("History directory " + historyLogDir.toString()
                + "does not exist");
      }

    } catch (Exception e) {
      throw new IOException("Not able to initialize History viewer", e);
    }
  }

  public void printJobInfoLoop() throws IOException {

    Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(historyLogDir,
            jobLogFileFilter));
    for (int i = 0; i < jobFiles.length; i++) {

      if (fs.getFileStatus(jobFiles[0]).getLen() == 0) {
        continue;
      } else {
        String[] jobDetails =
                JobHistory.JobInfo.decodeJobHistoryFileName(jobFiles[i].getName()).
                        split("_");

        String jobId = jobDetails[0] + "_" + jobDetails[1] + "_" + jobDetails[2];
        JobHistory.JobInfo job = new JobHistory.JobInfo(jobId);
        MVJobHistoryParser.parseJobTasks(jobFiles[i].toString(), job, fs);
        toJson(job);
      }
    }

  }


  public void toJson(JobHistory.JobInfo job) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    System.out.println(objectMapper.writeValueAsString(job));
  }

}
