package org.apache.flink.tez.runtime.input;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class FlinkInputSplitGenerator extends InputInitializer {

    InputFormat format;

    public FlinkInputSplitGenerator(InputInitializerContext initializerContext) {
        super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {

        Configuration tezConf = TezUtils.createConfFromUserPayload(this.getContext().getUserPayload());

        TezTaskConfig taskConfig = (TezTaskConfig) EncodingUtils.decodeObjectFromString(tezConf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());

        this.format = taskConfig.getInputFormat();

        InputSplit[] splits = format.createInputSplits((this.getContext().getNumTasks() > 0) ? this.getContext().getNumTasks() : 1 );

        LinkedList<Event> events = new LinkedList<Event>();
        for (int i = 0; i < splits.length; i++) {
            byte [] bytes = InstantiationUtil.serializeObject(splits[i]);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            InputDataInformationEvent event = InputDataInformationEvent.createWithSerializedPayload(i, buf);

            events.add(event);
        }
        return events;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {

    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
        //super.onVertexStateUpdated(stateUpdate);

    }
}
