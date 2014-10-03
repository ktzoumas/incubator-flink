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

package org.apache.flink.tez.dag;

import org.apache.flink.tez.runtime.input.FlinkUnorderedKVInput;
import org.apache.tez.runtime.library.conf.UnorderedKVInputConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVOutputConfig;

import java.util.Map;


public class FlinkUnorderedPartitionedKVEdgeConfig extends UnorderedPartitionedKVEdgeConfig {


	protected  FlinkUnorderedPartitionedKVEdgeConfig(
			UnorderedPartitionedKVOutputConfig outputConfiguration,
			UnorderedKVInputConfig inputConfiguration) {
		super (outputConfiguration, inputConfiguration);
	}

	/**
	 * Create a builder to configure the relevant Input and Output
	 * @param keyClassName the key class name
	 * @param valueClassName the value class name
	 * @return a builder to configure the edge
	 */

	public static Builder newBuilder(String keyClassName, String valueClassName,
									String partitionerClassName) {
		return new Builder(keyClassName, valueClassName, partitionerClassName, null);
	}

	@Override
	public String getInputClassName() {
		return FlinkUnorderedKVInput.class.getName();
	}

	public static class Builder extends UnorderedPartitionedKVEdgeConfig.Builder {

		protected Builder(String keyClassName, String valueClassName, String partitionerClassName,
					Map<String, String> partitionerConf) {
			super (keyClassName, valueClassName, partitionerClassName, partitionerConf);

		}

		public FlinkUnorderedPartitionedKVEdgeConfig build() {
			return new FlinkUnorderedPartitionedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
		}

	}
}
