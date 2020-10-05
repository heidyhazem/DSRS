package Partitionning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class SplittingAndReplication {

    private DataStream<Tuple3<String, String, Float>> inputStream;


    /**
     Constructs the input nonKeyedStream
     @param nonKeyedStream the non-keyed input dataStream
     */
    public SplittingAndReplication (DataStream<Tuple3<String, String, Float>> nonKeyedStream){
        inputStream = nonKeyedStream;
    }



    /**
     partitioning as batch by generating key using splitting and replication mechanism technique
     @param inputStream the non-keyed input dataStream
     @return the input stream with a key
     */
    public  DataStream<Tuple4<Integer,String,String,Float>> generateOneKey(DataStream<Tuple3<String, String, Float>> inputStream,Integer ni){

        DataStream<Tuple4<Integer,String,String,Float>> withKeyStream = inputStream.flatMap(new FlatMapFunction<Tuple3<String, String, Float>, Tuple4<Integer, String, String, Float>>() {

            Integer nodes = ni;
            Integer interactionHashNumber;

            @Override
            public void flatMap(Tuple3<String, String, Float> input, Collector<Tuple4<Integer, String, String, Float>> out) throws Exception {

                //Assign any numbers of nodes but it should be even.
                //Integer nodes = Integer.valueOf(params.get("ni"));

                ArrayList<Integer> usersNodes = new ArrayList<>();
                ArrayList<Integer> itemNodes = new ArrayList<>();

                //Hash user
                //nodes = userId%n + (0:n-1)n
                Long userID = Long.parseLong(input.f0);
                Long userHashNumber = userID % nodes;
                for (int x = 0; x <= nodes - 1; x++) {
                    Integer k = (int) (userHashNumber + (x * nodes));
                    usersNodes.add(k);
                    //out.collect(Tuple5.of(k,"user",input.f1,input.f2,poissonNumber));
                }


                //hash item
                //nodes = (itemId%n)n + (0:n-1)
                Long itemID = Long.parseLong(input.f1);
                Long itemHashNumber = itemID % nodes;
                for (int x = 0; x <= nodes - 1; x++) {
                    Integer k = (int) ((itemHashNumber * nodes) + x);
                    itemNodes.add(k);
                    //out.collect(Tuple5.of(k,"item",input.f1,input.f2,poissonNumber));
                }


                //get the common hash number and assign it
                for (Integer useNo : usersNodes) {
                    for (Integer itemNo : itemNodes) {
                        if (useNo.equals(itemNo)) {
                            interactionHashNumber = useNo;
                            break;
                        }
                    }
                }

                out.collect(Tuple4.of(340*interactionHashNumber, input.f0, input.f1, input.f2));

            }
        });

        return withKeyStream;
    }
}
