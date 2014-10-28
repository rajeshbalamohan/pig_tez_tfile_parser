package org.pig.udf.logs;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pig.udf.Utils;

import java.io.IOException;

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
 * Parse a log4j line and return a tuple
 */
public class Log4jParser extends EvalFunc<Tuple> {
  TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }

    String line = ((String) input.get(0)).trim();
    Utils.Log4jData data = Utils.parseLine(line);
    if (data != null) {
      Tuple tuple = tupleFactory.newTuple(5);
      tuple.set(0, data.time);
      tuple.set(1, data.logLevel);
      tuple.set(2, data.threadName);
      tuple.set(3, data.packageName);
      tuple.set(4, data.message);
      return tuple;
    }
    return null;
  }

  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("time", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("logLevel", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("threadName", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("packageName", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("message", DataType.CHARARRAY));
      return new Schema(new Schema.FieldSchema("data", tupleSchema));
    } catch (Exception e) {
      return null;
    }
  }
}
