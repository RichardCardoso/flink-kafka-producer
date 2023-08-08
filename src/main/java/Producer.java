import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Map;

public class Producer {

    public static KafkaSink<String> producer(Map<String, String> params) {

        String host = "kafka";
        String port = "29092";

        if (!StringUtils.isEmpty(params.get("host"))) {
            host = params.get("host");
        }
        if (!StringUtils.isEmpty(params.get("port"))) {
            port = params.get("port");
        }

        KafkaRecordSerializationSchema<String> serializationSchema = KafkaRecordSerializationSchema.<String>builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic("test")
                .build();

        return KafkaSink.<String>builder()
                .setBootstrapServers(host + ":" + port)
                .setRecordSerializer(serializationSchema)
                .build();
    }

    public static class MyNumberSequenceSource extends RichParallelSourceFunction<String> {

        private final long interval;

        public MyNumberSequenceSource(long interval) {

            this.interval = interval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            int counter = 1;
            boolean trigger = false;
            while(true) {
                if (!trigger || counter % 5 == 0) {
                    trigger = !trigger;
                    counter = 1;
                } else {
                    counter++;
                }
                if (trigger) {
                    ctx.collect(String.valueOf(3000));
                } else {
                    ctx.collect(String.valueOf(0));
                }
                Thread.sleep(interval);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
