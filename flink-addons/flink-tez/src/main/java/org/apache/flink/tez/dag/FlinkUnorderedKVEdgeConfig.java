/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.tez.dag;

import org.apache.flink.tez.runtime.input.FlinkUnorderedKVInput;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVInputConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVOutputConfig;


public class FlinkUnorderedKVEdgeConfig extends UnorderedKVEdgeConfig {


	private FlinkUnorderedKVEdgeConfig(
			UnorderedKVOutputConfig outputConfiguration,
			UnorderedKVInputConfig inputConfiguration)
	{
		super(outputConfiguration, inputConfiguration);
	}

	/**
	 * Create a builder to configure the relevant Input and Output
	 * @param keyClassName the key class name
	 * @param valueClassName the value class name
	 * @return a builder to configure the edge
	 */
	public static Builder newBuilder(String keyClassName, String valueClassName) {
		return new Builder(keyClassName, valueClassName);
	}

	@Override
	public String getInputClassName() {
		return FlinkUnorderedKVInput.class.getName();
	}

	public static class Builder extends UnorderedKVEdgeConfig.Builder {

		@InterfaceAudience.Private
		Builder(String keyClassName, String valueClassName) {
			super(keyClassName, valueClassName);
		}

		public FlinkUnorderedKVEdgeConfig build() {
			return new FlinkUnorderedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
		}

	}

}