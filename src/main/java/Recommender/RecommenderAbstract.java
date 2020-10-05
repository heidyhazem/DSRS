package Recommender;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Map;

/**
 represents how to implement recommender algorithm
 */
public abstract class RecommenderAbstract {


    /**
     *To recommend recommendation list for the user
     * @param withKeyStream The input data stream Keyed for partitioning(key,user,item,rate)
     * @param k  count of similar items/users
     * @return Map of predicted rates for the items  user has not rated
     */
    public abstract DataStream<Tuple3<String,String,Map<String,Float>>> fit(DataStream<Tuple4<Integer,String,String,Float>> withKeyStream, Integer k);




    /**
     *
     * @param withKeyStream The input data stream Keyed for partitioning(key,user,item,rate)
     * @param k count of similar items/users
     * @param forgettingTechnique the forgetting technique should be used
     * @return  Map of predicted rates for the items  user has not rated
     */
    public abstract DataStream<Tuple3<String,String,Map<String,Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k,
                                                                             String forgettingTechnique, Long window, Long userIdleThreshold, Long itemIdleThreshold);



    /**
     *To recommend recommendation list for the user
     * @param estimatedRatesOfItems The predicted rates for the items  user has not rated
     * @param k number ot recommended items
     * @return ArrayList of the recommended items
     */
    public abstract DataStream<Tuple3<String,String,ArrayList<String>>> recommend(DataStream<Tuple3<String,String,Map<String,Float>>> estimatedRatesOfItems, Integer k);


}