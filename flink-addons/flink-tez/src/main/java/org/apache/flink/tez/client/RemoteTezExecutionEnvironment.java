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

package org.apache.flink.tez.client;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.tez.dag.TezDAGGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

import java.util.HashMap;

public class RemoteTezExecutionEnvironment extends ExecutionEnvironment {

    private class RemoteTezRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {

            String jobName = args[0];

            Configuration conf = getConf();

            TezConfiguration tezConf;
            if (conf != null) {
                tezConf = new TezConfiguration(conf);
            } else {
                tezConf = new TezConfiguration();
            }

            UserGroupInformation.setConfiguration(tezConf);

            try {
                TezClient tezClient = TezClient.create(jobName, tezConf);

                tezClient.start();

                try {
                    Plan p = createProgramPlan(jobName);
                    PactCompiler compiler = new PactCompiler(null, new DefaultCostEstimator());
                    OptimizedPlan plan = compiler.compile(p);
                    TezDAGGenerator dagGenerator = new TezDAGGenerator(tezConf, new org.apache.flink.configuration.Configuration());
                    DAG dag = dagGenerator.createDAG(plan);


                    tezClient.waitTillReady();
                    System.out.println("Submitting DAG to Tez Client");
                    DAGClient dagClient = tezClient.submitDAG(dag);
                    System.out.println("Submitted DAG to Tez Client");

                    // monitoring
                    DAGStatus dagStatus = dagClient.waitForCompletion();

                    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
                        System.out.println(jobName + " failed with diagnostics: " + dagStatus.getDiagnostics());
                        System.exit(1);
                    }
                    System.out.println(jobName + " finished successfully");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    tezClient.stop();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        ToolRunner.run(new Configuration(), new RemoteTezRunner(), new String[] {jobName});
        return null;
    }

    @Override
    public String getExecutionPlan() throws Exception {
        return null;
    }

    public static RemoteTezExecutionEnvironment create () {
        return new RemoteTezExecutionEnvironment();
    }

    public RemoteTezExecutionEnvironment() {
    }
}
