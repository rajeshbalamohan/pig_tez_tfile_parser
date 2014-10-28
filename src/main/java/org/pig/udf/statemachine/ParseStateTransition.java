package org.pig.udf.statemachine;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
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

/**
 * ParseStateTransition(line)
 */
//TODO: Refactor later for ease of use. (Make it as groovy udf)
public class ParseStateTransition extends EvalFunc<Tuple> {

  private TupleFactory factory = TupleFactory.getInstance();

  //examples..
  //AMContainer container_1411511669099_0957_01_000082 transitioned from STOPPING to COMPLETED via event C_COMPLETED
  //attempt_1413172296052_0005_2_01_000000_0 TaskAttempt Transitioned from NEW to START_WAIT due to event TA_SCHEDULE
  //task_1413172296052_0005_2_00_000000 Task Transitioned from RUNNING to SUCCEEDED
  private Pattern pattern = Pattern.compile("(.*)[t|T]ransitioned from ([^ ]*) to ([^ ]*)");

  @Override public Tuple exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() != 1) {
      return null;
    }

    String str = (String) tuple.get(0);
    if (str == null) {
      return null;
    }

    Matcher matcher = pattern.matcher(str);
    Tuple t = null;
    while (matcher.find() && matcher.groupCount() >= 3) {
      t= factory.newTuple(3);
      t.set(0, matcher.group(1).trim()); //attempt_1413172296052_0005_2_01_000000_0 TaskAttempt
      t.set(1, matcher.group(2).trim()); //from event
      t.set(2, matcher.group(3).trim()); //to event
    }
    return t;
  }

  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("desc", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("from", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("to", DataType.CHARARRAY));
      return new Schema(new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE));
    } catch (Exception e) {
      return null;
    }
  }

}
