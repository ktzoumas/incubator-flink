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

package org.apache.flink.tez.runtime.input;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlinkInputSplitProvider implements InputSplitProvider, Serializable {

	private InputFormat format;
	private int minNumSplits;
	private final List<InputSplit> splits = new ArrayList<InputSplit>();


	public FlinkInputSplitProvider(InputFormat format, int minNumSplits) {
		this.format = format;
		this.minNumSplits = minNumSplits;
		initialize ();
	}

	private void initialize () {
		try {
			InputSplit [] splits = format.createInputSplits(minNumSplits);
			Collections.addAll(this.splits, splits);
		}
		catch (IOException e) {
			throw new RuntimeException("Could initialize InputSplitProvider");
		}

	}


	@Override
	public InputSplit getNextInputSplit() {
		InputSplit next = null;

		// keep the synchronized part short
		synchronized (this.splits) {
			if (this.splits.size() > 0) {
				next = this.splits.remove(this.splits.size() - 1);
			}
		}

		return next;
	}
}
