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

package org.apache.flink.tez.test;

import org.apache.flink.tez.examples.TPCHQuery3;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class TPCHQuery3ITCase extends TezProgramTestBase {

    protected String lineitemPath = "/tmp/" + "lineitem.tbl";
    protected String customerPath = "/tmp/" + "customer.tbl";
    protected String ordersPath = "/tmp/" + "orders.tbl";
    protected String resultPath;
    protected String expectedResultPath = "/tmp/" + "tpch3_expected_result.tbl";

    @Override
    protected void preSubmit() throws Exception {
        resultPath = getTempDirPath("result");
        this.setDegreeOfParallelism(1);
    }

    @Override
    protected void postSubmit() throws Exception {
        compareFiles(resultPath, expectedResultPath);
    }

    @Override
    protected void testProgram() throws Exception {
        TPCHQuery3.main(new String[]{lineitemPath, customerPath, ordersPath, resultPath});
    }

    private void compareFiles (String firstPath, String secondPath)  {

        try {
            ArrayList<String> firstList = new ArrayList<String>();
            readAllResultLines(firstList, firstPath, false);

            ArrayList<String> secondList = new ArrayList<String>();
            readAllResultLines(secondList, secondPath, false);

            String[] first = (String[]) firstList.toArray(new String[firstList.size()]);
            Arrays.sort(first);

            String[] second = (String[]) secondList.toArray(new String[secondList.size()]);
            Arrays.sort(second);

            Assert.assertEquals("Different number of lines in expected and obtained result.", first.length, second.length);
            Assert.assertArrayEquals(first, second);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit (-1);
        }
    }
}
