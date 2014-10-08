package org.apache.flink.tez.examples;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.runtime.DataSourceProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.runtime.input.FlinkInput;
import org.apache.flink.tez.runtime.input.FlinkInputSplitGenerator;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.ProcessorContext;

public class InputSplitTest {

    public static int DOP = 4;


    public static void main (String [] args) throws Exception {

        TezTaskConfig taskConfig = new TezTaskConfig(new Configuration());


        TezConfiguration conf = new TezConfiguration();

        conf.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(taskConfig));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                DataSourceProcessor.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

        Vertex dataSource = Vertex.create("Data Source", descriptor, DOP);


        InputDescriptor inputDescriptor = InputDescriptor.create(FlinkInput.class.getName());
        InputInitializerDescriptor inputInitializerDescriptor = InputInitializerDescriptor.create(FlinkInputSplitGenerator.class.getName());
        DataSourceDescriptor dataSourceDescriptor = DataSourceDescriptor.create(
                inputDescriptor,
                inputInitializerDescriptor,
                new Credentials()
        );
        dataSource.addDataSource("Input", dataSourceDescriptor);

    }

}
