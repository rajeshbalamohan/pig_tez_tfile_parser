package org.pig.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * ProcessCompletedLines("hdfs_location_mapping_file", line, "completed / http / EndToEnd")
 */

//TODO: Refactor later for ease of use. Dirty hack to get bad machines from the logs
public class ProcessCompletedLines extends EvalFunc<String> {

  Map<String, String> machineMapping = new HashMap<String, String>();

  SimpleDateFormat format =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");

  @Override public String exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() != 3) {
      return null;
    }

    String str = (String) tuple.get(0);
    if (str == null) {
      return null;
    }

    try {
      //Load machine mappings if needed
      if (machineMapping.isEmpty()) {
        String mappingFile = (String) tuple.get(1);
        populateMapping(mappingFile);
      }

      //Split the input line into words
      String[] data = str.split(" ");
      List<String> dataList = new LinkedList<String>();
      for (String s : data) {
        if (!s.isEmpty()) {
          dataList.add(s.trim());
        }
      }

      if (dataList.size() == 0) {
        return null;
      }

      String option = (String) tuple.get(2);
      //If we need per attempt info, try to check if  we have "Completed/CompressedSize" in the input line
      if (option.trim().equalsIgnoreCase("completed") && str.contains("Completed") && str.contains
          ("CompressedSize")) {
        return processCompleted(dataList);
      }

      //If we need src to attemptInfo mapping, try to check if  we have "http://"
      //This is under the assumption that higher level would have already filtered out fetcher data
      if (option.trim().equalsIgnoreCase("http") && str.contains("http://")) {
        return processHttp(dataList);
      }

      //If we need to parse end-to-end timing, check for freed keyword in input line
      if (option.trim().equalsIgnoreCase("EndToEnd") && str.contains("freed")) {
        return processEndToEndTime(dataList);
      }

    } catch (Exception e) {
      System.out.println("(will proceed)..Error parsing " + str);
      e.printStackTrace();
    }
    return null;
  }

  private void populateMapping(String mappingFile) throws IOException {
    Path path = new Path(mappingFile);
    FileSystem fs = path.getFileSystem(new Configuration());
    FSDataInputStream fin = fs.open(path);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
    while (reader.ready()) {
      String line = reader.readLine();
      String[] data = line.split(",");
      machineMapping.put(data[0], data[1]);
    }
  }

  public static String cleanMachineName(String machineName) {
    machineName = machineName.replaceAll("]", "");
    if (machineName.indexOf("/") > 0) {
      machineName = machineName.substring(0, machineName.indexOf("/"));
    }
    if (machineName.indexOf(":") > 0) {
      machineName = machineName.substring(0, machineName.indexOf(":"));
    }
    return machineName;
  }

  /**
   * TODO: Dirty way to parse the input data..As of now, its fine
   *
   * @param data
   * @return
   * @throws java.io.IOException
   * @throws java.text.ParseException
   */
  public String processCompleted(List<String> data) throws IOException, ParseException {
    String date = data.get(0) + " " + data.get(1);
    String vertexName = data.get(4);
    String threadId = data.get(5);
    String srcMachine = cleanMachineName(data.get(6));

    String machineId = machineMapping.get(srcMachine);

    String pathComponent = data.get(16).replaceAll("]", "").replaceAll("pathComponent=", "");
    String compressedSize = data.get(19).replaceAll("CompressedSize=", "").replaceAll(",", "");
    String decompressedSize = data.get(20);
    String timeTaken = data.get(21).replaceAll("TimeTaken=", "").replaceAll(",", "");

    String rate = data.get(22).replaceAll("Rate=", "");
    return (format.parse(date).getTime() + "," + vertexName + "," + threadId + "," + machineId + ","
        +
        pathComponent + "," + timeTaken + "," + rate + "," + compressedSize);
  }

  public String processHttp(List<String> data) throws IOException, ParseException {
    String date = data.get(0) + " " + data.get(1);
    String vertexName = data.get(4);
    String threadId = data.get(5);
    String srcMachine = cleanMachineName(data.get(6));

    String srcMachineId = machineMapping.get(srcMachine);

    URL url = new URL(data.get(9).replaceAll("url=", ""));
    String destMachineId = machineMapping.get(url.getHost());

    return (format.parse(date).getTime() + "," + vertexName + "," + threadId + "," + srcMachineId
        + ","
        + destMachineId + "," + url.getQuery());
  }

  //2014-09-25 18:02:07,830 INFO [fetcher [Map_5] #25 cn041-10.l42scl.hortonworks.com/172.19.128.41] org.apache.tez.runtime.library.common.shuffle.impl.ShuffleScheduler: cn041-10.l42scl:13562 freed by fetcher [Map_5] #25 cn041-10
  public String processEndToEndTime(List<String> data) throws IOException, ParseException {
    String date = data.get(0) + " " + data.get(1);
    String vertexName = data.get(4);
    String threadId = data.get(5);

    String srcMachineId = machineMapping.get(cleanMachineName(data.get(6)));
    String destMachineId = machineMapping.get(cleanMachineName(data.get(8)));

    String timeTaken = data.get(16).replaceAll("ms", "").replaceAll(",", "");

    return (format.parse(date).getTime() + "," + vertexName + "," + threadId + "," + srcMachineId
        + ","
        + destMachineId + "," + timeTaken);
  }

}
