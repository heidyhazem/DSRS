package Evaluation;

import java.util.ArrayList;

public class EvaluationMethods {


    /**
     *Incremental precision for online recommendation
     * @param consumedItem the current consumed item by the user
     * @param recommendedList Recommendation list for the user
     * @return precision = |E ∩ L| / k
     */

    Float precisionOnline(String consumedItem, ArrayList<String> recommendedList){

        Integer k = recommendedList.size();
        if (recommendedList.contains(consumedItem)){
            return (float)1/k;
        }
        else {
            return 0f;
        }
    }

    /**
     * Incremental recall for online recommendation
     * @param consumedItem the current consumed item by the user
     * @param recommendedList Recommendation list for the user
     * @return /Recall = |E ∩ L| / |E|
     */
    Integer recallOnline(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            //1/1
            return 1;
        }
        else {
            //0/1
            return 0;
        }
    }

    /**
     * Incremental ClickThrough for online recommendation
     * @param consumedItem  the current consumed item by the user
     * @param recommendedList Recommendation list for the user
     * @return ClickThrough = 1 if E∩L ̸= 0/  and zero otherwise;
     */
    //because of online case it is identical to recall
    Integer clickThrough(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            return 1;
        }
        else {
            return 0;
        }
    }

    /**
     * reciprocalRank is considering the position of the relevant item
     * @param consumedItem the current consumed item by the user
     * @param recommendedList Recommendation list for the user
     * @return reciprocalRank
     */
    Float reciprocalRank(String consumedItem, ArrayList<String> recommendedList){
        if (recommendedList.contains(consumedItem)){
            Integer rank = recommendedList.indexOf(consumedItem);
            return (float)1/rank;
        }
        else {
            return 0f;
        }
    }

    /**
     * Discounted cumulative gain DCG@K =  ∑ rel(ik)/log (1+k)
     * @param consumedItem the current consumed item by the user
     * @param recommendedList Recommendation list for the user
     * @return DisCumulativeGain
     */
    Double DisCumulativeGain(String consumedItem, ArrayList<String> recommendedList){

        if (recommendedList.contains(consumedItem)){
            Integer k = recommendedList.indexOf(consumedItem);
            Double rank = (double)1+k;
            return 1/(Math.log(rank)/Math.log(2));
        }
        else {
            return 0d;
        }
    }


}
