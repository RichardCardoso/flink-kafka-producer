import functions.MyLiveMessageSource;
import models.LiveMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import serialization.LiveMessageSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class KafkaFlinkApplication {

    public static void main(String[] args) throws Exception {

        Map<String, String> params = new HashMap<>();

        Stream.of(args)
                .filter(x -> !StringUtils.isEmpty(x))
                .filter(x -> x.indexOf(":") > 0 && x.indexOf(":") < x.length() - 1)
                .map(x -> x.split(":"))
                .forEach(x -> params.put(x[0], x[1]));

        System.out.println("Params: " + params);

        // Configure execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataSource
        DataStream<LiveMessage> numbers = env.addSource(new MyLiveMessageSource(1000)).setParallelism(1);
        numbers.sinkTo(new Producer<LiveMessage, LiveMessageSchema>().producer(params, "live", new LiveMessageSchema()));

        numbers.print();

        System.out.println("Starting flink job - producer");

        env.execute();
    }
}
