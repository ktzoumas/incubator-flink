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

package org.apache.flink.tez.runtime;


import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;

public class TezRuntimeEnvironment {

	private final IOManager ioManager;

	private final MemoryManager memoryManager;

	public TezRuntimeEnvironment(long totalMemory) {
		int pageSize = 32768;
		this.memoryManager = new DefaultMemoryManager(totalMemory, 10, pageSize);
		this.ioManager = new IOManager();
	}

	public IOManager getIOManager() {
		return ioManager;
	}

	public MemoryManager getMemoryManager() {
		return memoryManager;
	}
}
