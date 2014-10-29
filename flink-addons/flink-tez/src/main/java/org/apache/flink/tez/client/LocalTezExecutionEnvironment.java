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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.dag.TezDAGGenerator;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class LocalTezExecutionEnvironment extends ExecutionEnvironment{

	TezConfiguration tezConf;

	private LocalTezExecutionEnvironment() {
		this.tezConf = new TezConfiguration();
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.defaultFS", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
	}

	public static LocalTezExecutionEnvironment create() {
		return new LocalTezExecutionEnvironment();
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		try {
			TezClient tezClient = TezClient.create(jobName, tezConf);

			tezClient.start();

			try {
				Plan p = createProgramPlan(jobName);
				PactCompiler compiler = new PactCompiler(null, new DefaultCostEstimator());
				OptimizedPlan plan = compiler.compile(p);
				TezDAGGenerator dagGenerator = new TezDAGGenerator(tezConf, new Configuration());
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
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tezClient.stop();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		finally {
			return null;
		}
	}

	@Override
	public String getExecutionPlan() throws Exception {
		return null;
	}

    public void setAsContext() {
        initializeContextEnvironment(this);
    }

}
