package org.apache.flink.tez.runtime.input;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.InstantiationUtil;
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

        InputSplit[] splits = format.createInputSplits(this.getContext().getNumClusterNodes());

        LinkedList<Event> events = new LinkedList<Event>();
        for (int i = 0; i < splits.length; i++) {
            byte [] bytes = InstantiationUtil.serializeObject(splits[i]);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            InputDataInformationEvent event = new InputDataInformationEvent.createWithSerializedPayload(0, buf);
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
