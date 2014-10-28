package org.pig.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class Utils {

  static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");

  //2014-10-17 01:00:17,319 INFO [main] metrics.TaskCounterUpdater:  Using ResourceCalculatorProcessTree
  static Pattern tezLog4jPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}," +
      "\\d{3}) ([^ ]*) \\[(.*)\\] ([^ ]*): (.*)$");

  public static long parseDate(String log4jTime) throws ParseException {
    return format.parse(log4jTime).getTime();
  }

  //2014-09-25 18:02:07,830
  public static long parseDate(String date, String hours) throws ParseException {
    return parseDate(date + " " + hours);
  }

  public static Log4jData parseLine(String line) {
    List<String> returnList = new LinkedList<String>();
    Matcher matcher = tezLog4jPattern.matcher(line);
    Log4jData data = null;
    while (matcher.find() && matcher.groupCount() >= 5) {
      data = new Log4jData();
      data.time = matcher.group(1).trim();
      data.logLevel = matcher.group(2).trim();
      data.threadName = matcher.group(3).trim();
      data.packageName = matcher.group(4).trim();
      data.message = matcher.group(5).trim();
    }
    return data;
  }

  public static class Log4jData {
    public String time;
    public String logLevel;
    public String threadName;
    public String packageName;
    public String message;
  }
}
