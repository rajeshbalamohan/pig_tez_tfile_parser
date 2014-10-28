package org.pig.storage;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigConfiguration;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.TFileRecordWriter;
import org.apache.pig.impl.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class TFileStorage extends FileInputLoadFunc implements
    StoreFuncInterface, LoadMetadata {

  private static final Log LOG = LogFactory.getLog(TFileStorage.class);

  private TFileRecordReader recReader = null;
  private TFileRecordWriter recWriter = null;

  private BufferedReader bufReader;
  private Text currentKey;
  private TupleFactory tupleFactory = TupleFactory.getInstance();

  /**
   * We get the one complete TFile per KV read.
   * Add a BufferedReader so that we can scan a line at a time
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  private void setupReader() throws IOException, InterruptedException {
    if (recReader.nextKeyValue() && bufReader == null) {
      currentKey = recReader.getCurrentKey();
      Text val = recReader.getCurrentValue();
      bufReader = new BufferedReader(new StringReader(val.toString()));
    }
  }

  private String readLine() throws IOException, InterruptedException {
    String line = null;
    if (bufReader == null) {
      setupReader();
    }
    line = bufReader.readLine();
    if (line == null) {
      bufReader = null;
      setupReader();
      line = bufReader.readLine();
    }
    return line;
  }
  @Override
  public Tuple getNext() throws IOException {
    try {
      String line = readLine();
      if (line != null) {
        //machine, key, line
        Tuple tuple = tupleFactory.newTuple(3);
        if (currentKey != null) {
          String[] data = currentKey.toString().split(":");
          tuple.set(0, data[0]);
          tuple.set(1, data[1]);
        } else {
          tuple.set(0, "");
          tuple.set(1, "");
        }
        tuple.set(2, line);
        return tuple;
      }
    } catch (Exception e) {
      //TODO: handle this better
      return null;
    }
    return null;
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    try {
      recWriter.write(null, t);
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public static class TFileInputFormat extends
      PigFileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      return new TFileRecordReader();
    }

  }

  @Override
  public InputFormat getInputFormat() {
    return new TFileInputFormat();
  }

  @Override
  public int hashCode() {
    return 42;
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    recReader = (TFileRecordReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

  /**
   * Not needed at the moment, as we only read the data
   */
  public static class TFileOutputFormat
      extends
      FileOutputFormat<org.apache.hadoop.io.WritableComparable, Tuple> {

    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(
        TaskAttemptContext job) throws IOException,
        InterruptedException {
      Configuration conf = job.getConfiguration();
      String codec = conf.get(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "");
      if (!codec.equals("lzo") && !codec.equals("gz") && !codec.equals("gzip"))
        throw new IOException(
            "Invalid codec");
      if (codec.equals("gzip")) {
        codec = "gz";
      }
      LOG.info(codec + " compression codec in use");
      Path file = getDefaultWorkFile(job, "");
      return new TFileRecordWriter(file, codec, conf);
    }
  }

  @Override
  public OutputFormat getOutputFormat() {
    return new TFileOutputFormat();
  }

  @Override
  public void prepareToWrite(RecordWriter writer) {
    this.recWriter = (TFileRecordWriter) writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {

  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir)
      throws IOException {
    return LoadFunc.getAbsolutePath(location, curDir);
  }

  @Override
  public String[] getPartitionKeys(String location, Job job)
      throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job)
      throws IOException {
    return Utils.getSchema(this, location, true, job);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression plan) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    StoreFunc.cleanupOnFailureImpl(location, job);
  }

  @Override
  public void cleanupOnSuccess(String location, Job job) throws IOException {
  }
}
