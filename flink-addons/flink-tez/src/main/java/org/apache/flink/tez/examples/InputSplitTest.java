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


import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.tez.dag.FlinkUnorderedKVEdgeConfig;
import org.apache.flink.tez.dag.TezDAGGenerator;
import org.apache.flink.tez.runtime.DataSinkProcessor;
import org.apache.flink.tez.runtime.DataSourceProcessor;
import org.apache.flink.tez.runtime.DataSourceProcessorWithSplits;
import org.apache.flink.tez.runtime.RegularProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.runtime.input.FlinkInput;
import org.apache.flink.tez.runtime.input.FlinkInputSplitGenerator;
import org.apache.flink.tez.runtime.input.FlinkInputSplitProvider;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.tez.util.WritableSerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class InputSplitTest {

    public static int DOP = 4;

    static String jobName = "Input Split Tests";

    public static String INPUT_FILE="/tmp/hamlet.txt";

    public static String OUTPUT_FILE="/tmp/splits8";


    public static StringSerializer stringSerializer = new StringSerializer();

    public static StringComparator stringComparator = new StringComparator(true);

    public static TypeSerializer<Tuple2<String,Integer>> tupleSerializer = new TupleSerializer<Tuple2<String, Integer>>(
            (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
            new TypeSerializer[] {
                    new StringSerializer(),
                    new IntSerializer()
            }
    );

    public static TupleComparator<Tuple2<String,Integer>> tupleComparator = new TupleComparator<Tuple2<String, Integer>>(
            new int [] {0},
            new TypeComparator[] {new StringComparator(true)},
            new TypeSerializer[] {new StringSerializer()}
    );


    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }

    }

    private static Vertex createDataSink (TezConfiguration conf) throws Exception {
        TezConfiguration tezConf = new TezConfiguration(conf);
        TezTaskConfig sinkConfig = new TezTaskConfig(new Configuration());

        TextOutputFormat<Tuple2<String, Integer>> format =
                new TextOutputFormat<Tuple2<String, Integer>>(new Path(OUTPUT_FILE));
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);

        sinkConfig.setStubWrapper(new UserCodeObjectWrapper<TextOutputFormat>(format));
        sinkConfig.setInputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class), 0);

        tezConf.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(sinkConfig));
        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                DataSinkProcessor.class.getName());
        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(tezConf));
        Vertex vertex = Vertex.create("Data Sink", descriptor, DOP);
        return vertex;
    }

    private static Vertex createDataSource (TezConfiguration tezConf) throws Exception {
        TezTaskConfig dataSourceTaskConfig = new TezTaskConfig(new Configuration());
        dataSourceTaskConfig.setOutputSerializer(new RuntimeStatefulSerializerFactory<String>(new StringSerializer(), String.class));
        TextInputFormat inputFormat = new TextInputFormat(new Path(INPUT_FILE));
        dataSourceTaskConfig.setInputSplitProvider(new FlinkInputSplitProvider(inputFormat, DOP));
        dataSourceTaskConfig.setNumberSubtasksInOutput(new ArrayList<Integer>(Arrays.asList(DOP)));
        dataSourceTaskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
        dataSourceTaskConfig.setStubWrapper(new UserCodeObjectWrapper<TextInputFormat>(inputFormat));


        TezConfiguration dataSourceConfig = new TezConfiguration(tezConf);

        dataSourceConfig.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(dataSourceTaskConfig));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                DataSourceProcessorWithSplits.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(dataSourceConfig));

        Vertex dataSource = Vertex.create("Data Source", descriptor, DOP);


        InputDescriptor inputDescriptor = InputDescriptor.create(FlinkInput.class.getName());
        TezConfiguration inputConf = new TezConfiguration(tezConf);
        TezTaskConfig inputTaskConf = new TezTaskConfig(new Configuration());
        inputTaskConf.setInputFormat(inputFormat);
        inputTaskConf.setDatasourceProcessorName(dataSource.getName());
        inputConf.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(inputTaskConf));
        InputInitializerDescriptor inputInitializerDescriptor = InputInitializerDescriptor.create(FlinkInputSplitGenerator.class.getName()).setUserPayload(TezUtils.createUserPayloadFromConf(inputConf));
        DataSourceDescriptor dataSourceDescriptor = DataSourceDescriptor.create(
                inputDescriptor,
                inputInitializerDescriptor,
                new Credentials()
        );
        dataSource.addDataSource("Input", dataSourceDescriptor);

        return dataSource;
    }

    private static Vertex createTokenizer (TezConfiguration tezConf) throws Exception {

        ProcessorDescriptor tokenizerDescriptor = ProcessorDescriptor.create(RegularProcessor.class.getName());
        TezConfiguration tokenizerConf = new TezConfiguration(tezConf);
        TezTaskConfig tokenizerTaskConf = new TezTaskConfig(new Configuration());
        tokenizerTaskConf.setDriver(FlatMapDriver.class);
        tokenizerTaskConf.setDriverStrategy(DriverStrategy.FLAT_MAP);
        tokenizerTaskConf.setStubWrapper(new UserCodeClassWrapper<Tokenizer>(Tokenizer.class));
        tokenizerTaskConf.setStubParameters(new Configuration());
        tokenizerTaskConf.setInputSerializer(new RuntimeStatefulSerializerFactory<String>(stringSerializer, String.class), 0);
        tokenizerTaskConf.setInputComparator(new RuntimeComparatorFactory<String>(stringComparator), 0);
        tokenizerTaskConf.setOutputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class));
        tokenizerTaskConf.setOutputComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        tokenizerTaskConf.setInputLocalStrategy(0, LocalStrategy.NONE);
        tokenizerTaskConf.setNumberSubtasksInOutput(new ArrayList<Integer>(Arrays.asList(DOP)));
        HashMap<String,ArrayList<Integer>> inputPositions = new HashMap<String,ArrayList<Integer>>();
        inputPositions.put("Data Source", new ArrayList<Integer>(Arrays.asList(0)));
        tokenizerTaskConf.setInputPositions(inputPositions);
        tokenizerTaskConf.addOutputShipStrategy(ShipStrategyType.FORWARD);

        tokenizerConf.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(tokenizerTaskConf));
        tokenizerDescriptor.setUserPayload(TezUtils.createUserPayloadFromConf(tokenizerConf));
        Vertex tokenizer = Vertex.create("Tokenizer", tokenizerDescriptor, DOP);

        return tokenizer;
    }

    public static void main (String [] args) throws Exception {

        TezConfiguration tezConf = new TezConfiguration();

        tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
        tezConf.set("fs.defaultFS", "file:///");
        tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

        Vertex dataSource = createDataSource(tezConf);

        Vertex tokenizer = createTokenizer(tezConf);

        Vertex dataSink = createDataSink(tezConf);

        UnorderedKVEdgeConfig srcMapEdgeConf = (UnorderedKVEdgeConfig) (UnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", EncodingUtils.encodeObjectToString(new StringSerializer())))
                .done()
                .build();
        EdgeProperty srcMapEdgeProperty = srcMapEdgeConf.createDefaultOneToOneEdgeProperty();
        Edge edge1 = Edge.create(dataSource, tokenizer, srcMapEdgeProperty);

        UnorderedKVEdgeConfig tokenizerSinkConf = (UnorderedKVEdgeConfig) (UnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", EncodingUtils.encodeObjectToString(tupleSerializer)))
                .done()
                .build();

        EdgeProperty tokenizerSinkEdgeProperty = tokenizerSinkConf.createDefaultOneToOneEdgeProperty();

        Edge edge2 = Edge.create (tokenizer, dataSink, tokenizerSinkEdgeProperty);

        DAG dag = DAG.create(jobName);

        dag.addVertex(dataSource).addVertex(tokenizer).addVertex(dataSink).addEdge(edge1).addEdge(edge2);

        try {
            TezClient tezClient = TezClient.create(jobName, tezConf);

            tezClient.start();

            try {

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
                System.exit(0);
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
    }

}
