package org.apache.flink.tez.examples;


import org.apache.flink.api.common.Plan;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.dag.TezDAGGenerator;
import org.apache.flink.tez.runtime.DataSourceProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.runtime.input.FlinkInput;
import org.apache.flink.tez.runtime.input.FlinkInputSplitGenerator;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class InputSplitTest {

    public static int DOP = 4;

    static String jobName = "Input Split Tests";

    public static void main (String [] args) throws Exception {

        TezTaskConfig taskConfig = new TezTaskConfig(new Configuration());

        TezConfiguration tezConf = new TezConfiguration();


        tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
        tezConf.set("fs.defaultFS", "file:///");
        tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

        tezConf.set("io.flink.processor.taskconfig", EncodingUtils.encodeObjectToString(taskConfig));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                DataSourceProcessor.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(tezConf));

        Vertex dataSource = Vertex.create("Data Source", descriptor, DOP);


        InputDescriptor inputDescriptor = InputDescriptor.create(FlinkInput.class.getName());
        InputInitializerDescriptor inputInitializerDescriptor = InputInitializerDescriptor.create(FlinkInputSplitGenerator.class.getName());
        DataSourceDescriptor dataSourceDescriptor = DataSourceDescriptor.create(
                inputDescriptor,
                inputInitializerDescriptor,
                new Credentials()
        );
        dataSource.addDataSource("Input", dataSourceDescriptor);

        DAG dag = DAG.create(jobName);

        dag.addVertex(dataSource);

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
