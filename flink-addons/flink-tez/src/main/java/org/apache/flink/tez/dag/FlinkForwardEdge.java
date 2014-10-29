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


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.tez.util.WritableSerializationDelegate;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

import java.io.IOException;

public class FlinkForwardEdge extends FlinkEdge {

	public FlinkForwardEdge(FlinkVertex source, FlinkVertex target, TypeSerializer<?> typeSerializer) {
		super(source, target, typeSerializer);
	}

	@Override
	public Edge createEdge(TezConfiguration tezConf) {
		try {
            FlinkUnorderedKVEdgeConfig edgeConfig = (FlinkUnorderedKVEdgeConfig)
					(FlinkUnorderedKVEdgeConfig
							.newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
							.setFromConfiguration(tezConf)
							.configureInput()
							.setAdditionalConfiguration("io.flink.typeserializer", EncodingUtils.encodeObjectToString(
									this.typeSerializer
							)))
							.done()
							.build();

			EdgeProperty property = edgeConfig.createDefaultOneToOneEdgeProperty();
			this.cached = Edge.create(source.getVertex(), target.getVertex(), property);
			return cached;

		} catch (Exception e) {
			throw new CompilerException(
					"An error occurred while creating a Tez Forward Edge: " + e.getMessage(), e);
		}
	}
}
