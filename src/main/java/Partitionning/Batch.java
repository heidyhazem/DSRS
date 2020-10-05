package Partitionning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 includes methods for partitioning the input stream as batch (on one node)
 */
public class Batch {


    private DataStream<Tuple3<String, String, Float>> inputStream;


    /**
     Constructs the input nonKeyedStream
     @param nonKeyedStream the non-keyed input dataStream
     */
    public Batch (DataStream<Tuple3<String, String, Float>> nonKeyedStream){
        inputStream = nonKeyedStream;
    }

    /**
     partitioning as batch by generating the same key to all the records
     @param inputStream the non-keyed input dataStream
     @return the input stream with a key
     */

    public  DataStream<Tuple4<Integer,String,String,Float>> generateOneKey(DataStream<Tuple3<String, String, Float>> inputStream){

        DataStream<Tuple4<Integer,String,String,Float>> withKeyStream = inputStream.flatMap(new FlatMapFunction<Tuple3<String, String, Float>, Tuple4<Integer, String, String, Float>>() {
            @Override
            public void flatMap(Tuple3<String, String, Float> input, Collector<Tuple4<Integer, String, String, Float>> collector) throws Exception {
                Integer key = 0;
                collector.collect(Tuple4.of(key,input.f0,input.f1,input.f2));
            }
        });

        return withKeyStream;
    }


}
