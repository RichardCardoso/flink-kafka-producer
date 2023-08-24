import models.LiveMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producer<T, R extends SerializationSchema<T>> {

    public KafkaSink<T> producer(Map<String, String> params, String topic, R schema) {

        String host = "kafka";
        String port = "29092";

        if (!StringUtils.isEmpty(params.get("host"))) {
            host = params.get("host");
        }
        if (!StringUtils.isEmpty(params.get("port"))) {
            port = params.get("port");
        }

        KafkaRecordSerializationSchema<T> serializationSchema = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(schema)
                .setTopic(topic)
                .build();

        return KafkaSink.<T>builder()
                .setBootstrapServers(host + ":" + port)
                .setRecordSerializer(serializationSchema)
                .build();
    }
}
