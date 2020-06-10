package flinktest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.Random;

public class RandomGenerator implements SourceFunction<Tuple2<String,Integer>> {

    volatile boolean isRunning = true;
    final Logger log = LoggerFactory.getLogger(RandomGenerator.class);

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        Random random = new Random();
        int num = random.nextInt(100);

        while(isRunning) {
            ctx.collect(new Tuple2<>("count", num));
            log.info("Generate random number {} at {}", num, LocalTime.now());
            num = random.nextInt(100);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
