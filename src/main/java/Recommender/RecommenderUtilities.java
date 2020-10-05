package Recommender;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RecommenderUtilities {

    /**
     *
     * @param toBeOrderedMap Map of string as key and float as score to be ordered and retrieve most top k
     * @param k how many items/users to be retrieved as most similar
     * @return top k similar users/items
     */

    public ArrayList<Tuple2<String,Float>> getMostKuSimilar(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<Tuple2<String,Float>> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(Tuple2.of(thingSim.getKey(), thingSim.getValue())));

        return topKSimilar;

    }

    /**
     *
     * @param toBeOrderedMap Map of items and estimated rates
     * @param k howa many items to be recommended
     * @return top similar items
     */
    public ArrayList<String>topKItems(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<String> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(thingSim.getKey()));

        return topKSimilar;

    }

    /**
     *
     * @param allItems all items
     * @param topKSimilarthings top k similar items/users
     * @param userItemRatingHistory Map of user and his/her rated itesm
     * @return  Map of item and the corresponding estimated rate
     * @throws Exception
     */
    public Map<String,Float> estimateRateForItems (MapState<String,Integer> allItems,ArrayList<Tuple2<String,Float>> topKSimilarthings,
                                                   MapState<String,Map<String,Float>> userItemRatingHistory, String curentUser) throws Exception {

        Map<String,Float> estimatedRatesForItems = new HashMap<>();


        for (String itemInAll : allItems.keys()){

            //skipping already rated items by the user
            if( ! userItemRatingHistory.get(curentUser).containsKey(itemInAll)){
                //TODO: Handle Nulls
                Float denominator = 0f;
                Float numerator = 0f;

                for (Tuple2<String, Float> simUser : topKSimilarthings ){

                    Integer rateUserForItem;

                    //estimated rate
                    //to know what the rate for the other user to the item
                    if(userItemRatingHistory.get(simUser.f0).containsKey(itemInAll)){

                        rateUserForItem = 1;
                    }
                    else {
                        rateUserForItem = 0;
                    }

                    //numerator
                    numerator += rateUserForItem*(simUser.f1);

                    //denominator
                    denominator += simUser.f1;
                }

                Float estimatedRate = numerator/denominator;
                estimatedRatesForItems.put(itemInAll,estimatedRate);
            }

        }

        return estimatedRatesForItems;
    }



    public DataStream<Tuple3<Integer,String,ArrayList<String>>> recommend_Top_N(DataStream<Tuple3<Integer,String, Map<String, Float>>> estimatedRatesOfItems, Integer K) {

        DataStream<Tuple3<Integer,String,ArrayList<String>>> itemRecommList = estimatedRatesOfItems.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<Integer, String, Map<String, Float>>, Tuple3<Integer, String, ArrayList<String>>>() {
                    @Override
                    public void processElement(Tuple3<Integer, String, Map<String, Float>> input, Context context, Collector<Tuple3<Integer, String, ArrayList<String>>> out) throws Exception {

                        String item = input.f1;
                        ArrayList<String> recommendedItemsList = new RecommenderUtilities().topKItems(input.f2,K);

                        out.collect(Tuple3.of(input.f0,item,recommendedItemsList));
                    }
                });

        return itemRecommList;
    }

    public DataStream<Tuple3<String,String,ArrayList<String>>> recommend_Top_N2(DataStream<Tuple3<String,String, Map<String, Float>>> estimatedRatesOfItems, Integer K) {

        DataStream<Tuple3<String,String,ArrayList<String>>> itemRecommList = estimatedRatesOfItems.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, String, Map<String, Float>>, Tuple3<String, String, ArrayList<String>>>() {
                    @Override
                    public void processElement(Tuple3<String, String, Map<String, Float>> input, Context context, Collector<Tuple3<String, String, ArrayList<String>>> out) throws Exception {
                        String item = input.f1;
                        ArrayList<String> recommendedItemsList = new RecommenderUtilities().topKItems(input.f2,K);

                        out.collect(Tuple3.of(input.f0,item,recommendedItemsList));
                    }
                });


        return itemRecommList;
    }



    public static ArrayList<Tuple3<String,Float, Float>> getToKSimilarUserBased(MapState<Tuple2<String, String>, Float> pairItemCoRating,Map<String,Float> ratedItemsByUser, String item, Integer k) throws Exception {

        Map<String, Float> allRelatedItemsToTheItem = new HashMap<>();
        //<item,rate>,sim
        Map<Tuple2<String,Float>,Float> allRelatedItemsWithSim = new HashMap<>();
        //item,rate,sim
        ArrayList<Tuple3<String,Float, Float>> mostSimilarKItems = new ArrayList<>();

        //if( !(ratedItemsByUser.containsKey(item))){
        for (Map.Entry<String,Float> entryItem : ratedItemsByUser.entrySet()){
            String firstItemInPairinternal = String.valueOf(Math.min(Long.valueOf(item), Long.valueOf(entryItem.getKey())));
            String secondItemInPairinternal = String.valueOf(Math.max(Long.valueOf(item), Long.valueOf(entryItem.getKey())));
            if(pairItemCoRating.contains(Tuple2.of(firstItemInPairinternal,secondItemInPairinternal))){
                Float sim = pairItemCoRating.get(Tuple2.of(firstItemInPairinternal,secondItemInPairinternal));
                allRelatedItemsToTheItem.put(entryItem.getKey(),sim);
                //item, user edalo rate kam, sim
                allRelatedItemsWithSim.put(Tuple2.of(entryItem.getKey(),entryItem.getValue()),sim);
            }
        }

        allRelatedItemsWithSim.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //item,rate,sim
                .forEach(itemRate -> mostSimilarKItems.add(Tuple3.of(itemRate.getKey().f0,itemRate.getKey().f1, itemRate.getValue())));


        return (mostSimilarKItems);

       /* else {
            //as it is already rated by user so  need to check to recommend it
            return null;
        }*/
    }


    public static Float estimatRate_TenRec (ArrayList<Tuple3<String,Float, Float>> similaitiesList ) {

        Float estRate =0f;


        for (Tuple3<String, Float, Float> itemSim : similaitiesList) {

            Float numerator = 0f;
            Float denominator =0f;

            //sim * rate
            numerator +=  (itemSim.f1*itemSim.f2);

            //sum of rates
            denominator += itemSim.f1;

            estRate = numerator/denominator;
        }

        return estRate;

    }
}




