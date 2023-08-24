package serialization;

import models.LiveMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveMessageSchema implements SerializationSchema<LiveMessage> {

    private final static Logger log = LoggerFactory.getLogger(LiveMessageSchema.class);

    @Override
    public void open(InitializationContext context) throws Exception {

        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(LiveMessage liveMessage) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] bLiveMessage = mapper.writeValueAsBytes(liveMessage);
            return bLiveMessage;
        } catch (Exception ex) {
            log.error("Failed to write controlMessage ({}) as bytes", liveMessage);
            return null;
        }
    }
}
