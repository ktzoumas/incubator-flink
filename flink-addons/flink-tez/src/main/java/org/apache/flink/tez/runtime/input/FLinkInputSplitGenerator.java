package org.apache.flink.tez.runtime.input;


import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import java.util.List;

public class FlinkInputSplitGenerator extends InputInitializer {

    public FlinkInputSplitGenerator(InputInitializerContext initializerContext) {
        super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
        return null;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {

    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
        //super.onVertexStateUpdated(stateUpdate);

    }
}
