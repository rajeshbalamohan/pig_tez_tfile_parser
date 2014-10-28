package org.pig.udf.statemachine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
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
 * ((STOP_REQUESTED,STOPPING),(0.0,1.0,1.0,1.0,1.0,2.0,2.0,2.0,3.0,7.0,133.0))
 * Converts to format understanable by GnuPlot (i.e percentile, value)
 */
//TODO: Refactor later for ease of use.
public class QuantileToGNUFormat extends EvalFunc<DataBag> {

  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private BagFactory bagFactory = BagFactory.getInstance();

  static final Log LOG = LogFactory.getLog(QuantileToGNUFormat.class);

  static final Pattern pattern = Pattern.compile("\\((.*)\\),\\((.*)\\)");

  @Override public DataBag exec(Tuple input) throws IOException {
    DataBag bag = bagFactory.newDefaultBag();
    float percent = 0.1f;
    String line = (String) input.get(0);
    Matcher matcher = pattern.matcher(line);
    if (matcher.find() && matcher.groupCount() > 1) {
      String header = matcher.group(1);
      String contents = matcher.group(2);
      for(String data : contents.split(",")) {
        Tuple t = tupleFactory.newTuple(3);
        t.set(0, header);
        t.set(1, percent);
        percent += 0.1;
        t.set(2, data);
        bag.add(t);
      }
    }
    return bag;
  }


  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("header", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("percentile", DataType.FLOAT));
      tupleSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));
      return new Schema(new Schema.FieldSchema("transition", tupleSchema));
    } catch (Exception e) {
      return null;
    }
  }
}
