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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tez.client.TezExecutionEnvironment;
import org.apache.flink.util.Collector;


public  class WordCount {

	public static String INPUT_FILE="/tmp/sherlock.txt";
	public static String OUTPUT_FILE="/tmp/wordcount_output7";

	public static void main (String [] args) throws Exception {
		ExecutionEnvironment env = TezExecutionEnvironment.create();
		env.setDegreeOfParallelism(4);

		DataSet<String> text = env.readTextFile(INPUT_FILE);

		DataSet<Tuple2<String, Integer>> counts = text
				.flatMap(new Tokenizer())
				.groupBy(0)
				.reduce(new Summer());


		counts.writeAsCsv(OUTPUT_FILE, "\n", " ");

		env.execute();
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	public static final class Summer implements ReduceFunction<Tuple2<String,Integer>> {
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<String,Integer> (value1.f0, value1.f1 + value2.f1);
		}
	}
}
