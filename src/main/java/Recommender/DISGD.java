package Recommender;

import IncrementalMatrixFactorization.SGD;
import Recommender.RecommenderAbstract;
import Type_classes.userItem;
import VectorsHandlers.VectorOperations;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public  class DISGD extends RecommenderAbstract {

    public DataStream<Tuple3<String, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k,Double mu, Double lambda) {
        DataStream<Tuple3<String, String, Map<String, Float>>> itemsScores = withKeyStream.
                keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<String, String, Map<String, Float>>>() {

                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;


                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<String, String, Map<String, Float>>> out) throws Exception {

                        //Matrix factorization for each bag
                        String user = input.f1;
                        String item = input.f2;

                        SGD sgd = new SGD();
                        userItem userItemVectors;

                        Double[] itemVector;
                        Double[] userVector;
                        //Boolean knownUser = false;

                        Map<String, Float> itemsScoresMatrixMap = new HashMap<>();

                        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        //************************get or initialize user vector************************
                        if (usersMatrix.contains(user)) {
                            userVector = usersMatrix.get(user);
                            //knownUser = true;
                        }
                        //user is not known before.
                        //initialize it
                        else {
                            userVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //userVector = VectorOperations.initializeVector(k);
                        }
                        //*******************************************************************************
                        //************************get or initialize item vector*************************

                        if (itemsMatrix.contains(item)) {
                            itemVector = itemsMatrix.get(item);
                        }
                        //item is not known before.
                        //initialize it
                        else {

                            itemVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //itemVector = VectorOperations.initializeVector(latentFeatures);
                        }

                        //******************************************************************************
                        //if(knownUser) {
                        //*******************************1-recommend top k items for the user*****************************************
                        //rate the coming user with all items
                        //output it on the side

                        Iterable<Map.Entry<String, Double[]>> itemsVectors = itemsMatrix.entries();
                        for (Map.Entry<String, Double[]> anItem : itemsVectors) {
                            Double score = Math.abs(1 - VectorOperations.dot(userVector, anItem.getValue()));
                            itemsScoresMatrixMap.put(anItem.getKey(), score.floatValue());
                        }
                        //************************************************************************************************************
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        //send the maps to take the average then recommend and score

                        out.collect(Tuple3.of(user, item, itemsScoresMatrixMap));

                        // }
                        //*******************************3. update the model with the observed event**************************************
                        for (Integer l = 0; l < k; l++) {
                            userItemVectors = sgd.update_isgd(userVector, itemVector, mu, lambda);
                            usersMatrix.put(user, userItemVectors.userVector);
                            itemsMatrix.put(item, userItemVectors.itemVector);
                        }

                    }

                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Double[]> descriptor1 =
                                new MapStateDescriptor<>(
                                        "itemMatrixDescriptor",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, Double[]> descriptor2 =
                                new MapStateDescriptor<>(
                                        "usersMatrix",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, ArrayList<String>> descriptor3 =
                                new MapStateDescriptor<>(
                                        "ratedItemsByUser",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<ArrayList<String>>() {
                                        })
                                );

                        ValueStateDescriptor<Integer> descriptor4 =
                                new ValueStateDescriptor<Integer>(
                                        "flag",
                                        Integer.class

                                );


                        itemsMatrix = getRuntimeContext().getMapState(descriptor1);
                        usersMatrix = getRuntimeContext().getMapState(descriptor2);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                    }
                });

        return itemsScores;
    }


    public DataStream<Tuple3<String, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k,Double mu, Double lambda, String forgettingTechnique, Long window, Long userIdleThreshold, Long itemIdleThreshold) {
        DataStream<Tuple3<String, String, Map<String, Float>>> itemsScores = withKeyStream.
                keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<String, String, Map<String, Float>>>() {



                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;


                    //states for forgetting
                    ValueState<Long> generalCounter;
                    MapState<String, Long> userFootPrint;
                    MapState<String, Long> itemFootPrint;
                    private ValueState<Boolean> status;



                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context ctx, Collector<Tuple3<String, String, Map<String, Float>>> out) throws Exception {

                        //Matrix factorization for each bag
                        String user = input.f1;
                        String item = input.f2;

                        SGD sgd = new SGD();
                        userItem userItemVectors;

                        Double[] itemVector;
                        Double[] userVector;
                        //Boolean knownUser = false;

                        Map<String, Float> itemsScoresMatrixMap = new HashMap<>();

                        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        //************************get or initialize user vector************************
                        if (usersMatrix.contains(user)) {
                            userVector = usersMatrix.get(user);
                            //knownUser = true;
                        }
                        //user is not known before.
                        //initialize it
                        else {
                            userVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //userVector = VectorOperations.initializeVector(k);
                        }
                        //*******************************************************************************
                        //************************get or initialize item vector*************************

                        if (itemsMatrix.contains(item)) {
                            itemVector = itemsMatrix.get(item);
                        }
                        //item is not known before.
                        //initialize it
                        else {

                            itemVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //itemVector = VectorOperations.initializeVector(latentFeatures);
                        }

                        //******************************************************************************
                        //if(knownUser) {
                        //*******************************1-recommend top k items for the user*****************************************
                        //rate the coming user with all items
                        //output it on the side

                        Iterable<Map.Entry<String, Double[]>> itemsVectors = itemsMatrix.entries();
                        for (Map.Entry<String, Double[]> anItem : itemsVectors) {
                            Double score = Math.abs(1 - VectorOperations.dot(userVector, anItem.getValue()));
                            itemsScoresMatrixMap.put(anItem.getKey(), score.floatValue());
                        }
                        //************************************************************************************************************
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        //send the maps to take the average then recommend and score

                        out.collect(Tuple3.of(user, item, itemsScoresMatrixMap));

                        // }

                        //*******************************Forgetting tecghnique************************************************************

                        if(forgettingTechnique.equals("LFU")){


                            Boolean newItem = true;
                            Boolean newUser = true;
                            //update the count with the new record
                            Long count = generalCounter.value();
                            count += 1;
                            generalCounter.update(count);

                            if (userFootPrint.contains(user)) {
                                userFootPrint.put(user, userFootPrint.get(user) + 1);
                                newUser = false;
                            }

                            if (itemFootPrint.contains(item)) {
                                itemFootPrint.put(item, itemFootPrint.get(item) + 1);
                                newItem = false;
                            }

                            //1/4 170000

                            if (generalCounter.value().equals(window)) {
                                //reset
                                generalCounter.update(0L);

                                Map<String, Long> userFootPrintClone = new HashMap<>();
                                for(Map.Entry<String,Long> userCount : userFootPrint.entries()){
                                    userFootPrintClone.put(userCount.getKey(),userCount.getValue());
                                }


                                //userFootPrint.putAll(userFootPrintClone);

                                Map<String, Long> itemFootPrintClone = new HashMap<>();
                                //itemFootPrint.putAll(itemFootPrintClone);
                                for (Map.Entry<String, Long> itemCount : itemFootPrint.entries()) {
                                    itemFootPrintClone.put(itemCount.getKey(),itemCount.getValue());
                                }



                                for (Map.Entry<String, Long> userCount : userFootPrintClone.entrySet()) {

                                    //Step 1: remove user from from history
                                    //37 is the average visits by the user(analytics)
                                    if (userCount.getValue() < userIdleThreshold) {
                                        usersMatrix.remove(userCount.getKey());
                                        ratedItemsByUser.remove(userCount.getKey());
                                        userFootPrint.remove(userCount.getKey());
                                    }
                                }

                                for (Map.Entry<String, Long> itemCount : itemFootPrintClone.entrySet()) {
                                    if (itemCount.getValue() < itemIdleThreshold) {
                                        itemsMatrix.remove(itemCount.getKey());
                                        itemFootPrint.remove(itemCount.getKey());
                                    }
                                }


                                if (newUser) {
                                    userFootPrint.put(user, 1L);
                                }

                                if (newItem) {
                                    itemFootPrint.put(item, 1L);
                                }
                            }

                            /*Integer userCounts2 = 0;
                            for(String record : usersMatrix.keys()){
                                userCounts2+=1;
                            }
                           out.collect(Tuple3.of("u",userCounts2.toString(),new HashMap<>()));
*/
                        }
                        else if(forgettingTechnique.equals("LRU")){


                            if(status.value()){
                                //TODO: Remove this state
                                //if the first record only
                                generalCounter.update(ctx.timestamp());
                                status.update(false);
                            }

                            if(ctx.timestamp() >= generalCounter.value() + window){

                                generalCounter.update(ctx.timestamp());
                                Map<String, Long> userFootPrintClone = new HashMap<>();
                                for(Map.Entry<String,Long> userMap : userFootPrint.entries()){
                                    userFootPrintClone.put(userMap.getKey(),userMap.getValue());
                                }

                                for(Map.Entry<String,Long> userMap : userFootPrintClone.entrySet()){
                                    //forget users
                                    if(ctx.timestamp() >=  userMap.getValue()+userIdleThreshold){
                                        usersMatrix.remove(userMap.getKey());
                                        ratedItemsByUser.remove(userMap.getKey());
                                        userFootPrint.remove(userMap.getKey());
                                    }
                                }


                                Map<String, Long> itemFootPrintClone = new HashMap<>();
                                for(Map.Entry<String,Long> itemMap : itemFootPrint.entries()){
                                    itemFootPrintClone.put(itemMap.getKey(),itemMap.getValue());
                                }


                                for(Map.Entry<String,Long> itemMap : itemFootPrintClone.entrySet()){
                                    if(ctx.timestamp() >=  itemMap.getValue()  + itemIdleThreshold){

                                        itemsMatrix.remove(itemMap.getKey());
                                        itemFootPrint.remove(itemMap.getKey());
                                    }
                                }

                            }
                            userFootPrint.put(user,ctx.timestamp());
                            itemFootPrint.put(item,ctx.timestamp());


                            /*Integer userCounts2 = 0;
                            for(String record : usersMatrix.keys()){
                                userCounts2+=1;
                            }
                            out.collect(Tuple3.of("u",userCounts2.toString(),new HashMap<>()));
*/


                        }

                       /* Integer userCounts2 = 0;
                        for(String record : usersMatrix.keys()){
                            userCounts2+=1;
                        }
                        out.collect(Tuple3.of("u",userCounts2.toString(),new HashMap<>()));
*/



                        //*******************************3. update the model with the observed event**************************************
                        for (Integer l = 0; l < k; l++) {
                            userItemVectors = sgd.update_isgd(userVector, itemVector, mu, lambda);
                            usersMatrix.put(user, userItemVectors.userVector);
                            itemsMatrix.put(item, userItemVectors.itemVector);
                        }
                    }


                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Double[]> descriptor1 =
                                new MapStateDescriptor<>(
                                        "itemMatrixDescriptor",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, Double[]> descriptor2 =
                                new MapStateDescriptor<>(
                                        "usersMatrix",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, ArrayList<String>> descriptor3 =
                                new MapStateDescriptor<>(
                                        "ratedItemsByUser",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<ArrayList<String>>() {
                                        })
                                );

                        ValueStateDescriptor<Integer> descriptor4 =
                                new ValueStateDescriptor<Integer>(
                                        "flag",
                                        Integer.class

                                );

                        ValueStateDescriptor<Boolean> descriptor5 =
                                new ValueStateDescriptor<Boolean>(
                                        "status",
                                        Boolean.class,
                                        true
                                );

                        ValueStateDescriptor<Long> descriptor6 =
                                new ValueStateDescriptor<>(
                                        "GeneralCounter",
                                        Long.class,
                                        0L
                                );

                        MapStateDescriptor<String, Long> desriptor7 =
                                new MapStateDescriptor<String, Long>(
                                        "user(count)",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })

                                );

                        MapStateDescriptor<String, Long> desriptor8 =
                                new MapStateDescriptor<String, Long>(
                                        "item(count)",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })

                                );

                        itemsMatrix = getRuntimeContext().getMapState(descriptor1);
                        usersMatrix = getRuntimeContext().getMapState(descriptor2);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                        status = getRuntimeContext().getState(descriptor5);
                        generalCounter = getRuntimeContext().getState(descriptor6);
                        userFootPrint = getRuntimeContext().getMapState(desriptor7);
                        itemFootPrint = getRuntimeContext().getMapState(desriptor8);
                    }
                });


        return itemsScores;
    }

    @Override
    public DataStream<Tuple3<String, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k) {
        return fit(withKeyStream,k,0.05,0.01);
    }

    public DataStream<Tuple3<String, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingTechnique, Long window, Long userIdleThreshold, Long itemIdleThreshold) {
        return fit(withKeyStream,k,0.05,0.01,forgettingTechnique,window,userIdleThreshold,itemIdleThreshold);
    }

    public DataStream<Tuple3<String, String, ArrayList<String>>> recommend(DataStream<Tuple3<String, String, Map<String, Float>>> estimatedRatesOfItems, Integer k) {
        DataStream<Tuple3<String, String, ArrayList<String>>> recommendedItems = estimatedRatesOfItems.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, String, Map<String, Float>>, Tuple3<String, String, ArrayList<String>>>() {

                    MapState<String, Void> ratedItems;

                    @Override
                    public void processElement(Tuple3<String, String, Map<String, Float>> input, Context context, Collector<Tuple3<String, String, ArrayList<String>>> out) throws Exception {

                        Map<String, Float> AllitemsWithScore = new HashMap<>();
                        String currentUser = input.f0;
                        String currentItem = input.f1;

                        //************************************Avg Scores************************************************************************

                        for (Map.Entry<String, Float> userItemScore : input.f2.entrySet()) {
                            //TODO:Get the top n from each node
                            //test first with all items
                            if (ratedItems.contains(userItemScore.getKey())) {
                                continue;
                            } else {
                                AllitemsWithScore.put(userItemScore.getKey(), userItemScore.getValue());
                            }

                        }

                        //here we are ready for scoring and recommendation
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        ArrayList<String> recommendedItems = new ArrayList<>();
                        Integer N = 10;

                        AllitemsWithScore.entrySet().stream().sorted(Map.Entry.comparingByValue())
                                .limit(N)
                                .forEach(itemRate -> recommendedItems.add(itemRate.getKey()));

                        out.collect(Tuple3.of(currentUser,currentItem,recommendedItems));

                        ratedItems.put(currentItem, null);

                        //*********

                    }
                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Void> descriptor =
                                new MapStateDescriptor<>(
                                        "ratedItems",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Void>() {
                                        })
                                );

                        ratedItems = getRuntimeContext().getMapState(descriptor);
                    }
                });
        return recommendedItems;
    }
}