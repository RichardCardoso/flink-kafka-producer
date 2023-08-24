package functions;

import models.LiveMessage;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyLiveMessageSource extends RichParallelSourceFunction<LiveMessage> {

    private AtomicBoolean running = new AtomicBoolean(true);

    private final long interval;

    public MyLiveMessageSource(long interval) {

        this.interval = interval;
    }

    @Override
    public void run(SourceContext<LiveMessage> ctx) throws Exception {

        int counter = 1;
        boolean trigger = false;
        LiveMessage message = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while(running.get()) {
            if (!trigger || counter % 10 == 0) {
                trigger = !trigger;
                counter = 1;
            } else {
                counter++;
            }
            if (trigger) {
                message = new LiveMessage(3000, sdf.format(new Date()));
            } else {
                message = new LiveMessage(0, sdf.format(new Date()));
            }
            ctx.collect(message);
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {

        running.set(false);
    }
}