package flinktest;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WindowDemo {
    static final Logger log = LoggerFactory.getLogger(WindowDemo.class);

    public void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Integer>> intstream = env.addSource(new RandomGenerator());

        intstream.keyBy(0)
                .sum(1)
                .print();

        env.execute(this.getClass().getSimpleName());

    }




}


