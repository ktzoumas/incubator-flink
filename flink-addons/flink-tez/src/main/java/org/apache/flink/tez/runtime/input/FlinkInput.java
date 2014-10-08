package org.apache.flink.tez.runtime.input;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.InstantiationUtil;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

import java.util.List;


public class FlinkInput extends AbstractLogicalInput {

    InputSplit split;

    public FlinkInput(InputContext inputContext, int numPhysicalInputs) {
        super(inputContext, numPhysicalInputs);
        split = null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {
        Event event = inputEvents.iterator().next();
        Preconditions.checkArgument(event instanceof InputDataInformationEvent,
                getClass().getSimpleName()
                        + " can only handle a single event of type: "
                        + InputDataInformationEvent.class.getSimpleName());

        this.split = getSplitFromEvent ((InputDataInformationEvent)event);
    }

    InputSplit getSplitFromEvent (InputDataInformationEvent e) throws Exception {
        return (InputSplit) InstantiationUtil.deserializeObject(e.getUserPayload().array(), getClass().getClassLoader());
    }

    @Override
    public List<Event> close() throws Exception {
        return null;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public Reader getReader() throws Exception {
        return null;
    }

    @Override
    public List<Event> initialize() throws Exception {

        return null;
    }
}
