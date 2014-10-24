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

package org.apache.flink.tez.examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tez.client.LocalTezExecutionEnvironment;


public class InputSplitTest2 {

    private static String lineitemPath = "/tmp/lineitem.tbl";
    private static String outputPath = "/tmp/tpch3_test_out2";

    public static void main (String [] args) throws Exception {
        LocalTezExecutionEnvironment env = LocalTezExecutionEnvironment.create();
        env.setDegreeOfParallelism(4);

        DataSet<Lineitem> lineitem =  env.readCsvFile(lineitemPath)
                .fieldDelimiter('|')
                .includeFields("1000011000100000")
                .tupleType(Lineitem.class);

        lineitem.writeAsCsv(outputPath, "\n", "|");

        env.execute();
    }

    public static class Lineitem extends Tuple4<Integer, Double, Double, String> {

        public Integer getOrderkey() { return this.f0; }
        public Double getDiscount() { return this.f2; }
        public Double getExtendedprice() { return this.f1; }
        public String getShipdate() { return this.f3; }
    }

}
