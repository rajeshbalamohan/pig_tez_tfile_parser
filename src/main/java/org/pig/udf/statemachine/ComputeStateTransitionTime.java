package org.pig.udf.statemachine;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

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
 * ComputeStateTransitionTime(bag(tuple))
 * e.g: //Databag: V1 --> {(time, desc, from, to), (time, desc, from, to), (time, desc, from, to)..}
 */

//TODO: Refactor later for ease of use.
public class ComputeStateTransitionTime extends EvalFunc<DataBag> {

  private TupleFactory factory = TupleFactory.getInstance();
  BagFactory bagFactory = BagFactory.getInstance();

  static class Record {
    long time;
    String from;
    String to;
  }

  LinkedList<Record> records = new LinkedList<Record>();

  @Override public DataBag exec(Tuple t) throws IOException {
    if (t == null) {
      return null;
    }

    //Databag: V1 --> {(time, desc, from, to), (time, desc, from, to), (time, desc, from, to)..}
    Iterator<Tuple> iterator = ((DataBag) t.get(0)).iterator();
    while (iterator.hasNext()) {
      Tuple tuple = iterator.next();
      Record record = new Record();
      record.time = (Long) tuple.get(0);
      record.from = (String) tuple.get(2);
      record.to = (String) tuple.get(3);
      records.add(record);
    }
    DataBag returnBag = bagFactory.newDefaultBag();
    //Start processing
    Record prevRecord = null;
    long timeTaken = 0;
    while (records.size() > 0) {
      Record record = records.removeFirst();
      if (prevRecord == null) {
        //special case
        prevRecord = record;
        Tuple returnTuple = factory.newTuple(5);
        returnTuple.set(0, prevRecord.from);
        returnTuple.set(1, prevRecord.to);
        returnTuple.set(2, record.to);
        returnTuple.set(3, timeTaken);
        returnTuple.set(4, "");
        returnBag.add(returnTuple);
      } else {
        timeTaken = record.time - prevRecord.time;
        Tuple returnTuple = factory.newTuple(5);
        returnTuple.set(0, prevRecord.from);
        returnTuple.set(1, prevRecord.to);
        returnTuple.set(2, record.to);
        returnTuple.set(3, timeTaken);
        if (prevRecord.to.equalsIgnoreCase(record.from)) {
          returnTuple.set(4, "");
        } else {
          returnTuple.set(4, "Invalid Data : prevRecord.to=" + prevRecord.to + "; record.from=" +
              record.from);
        }
        returnBag.add(returnTuple);
      }
      prevRecord = record;
    }

    return returnBag;
  }

  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("from", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("intermediate", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("to", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("timeTaken", DataType.LONG));
      tupleSchema.add(new Schema.FieldSchema("errorMsg", DataType.CHARARRAY));
      return new Schema(new Schema.FieldSchema("transition", tupleSchema));
    } catch (Exception e) {
      return null;
    }
  }

}
