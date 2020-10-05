package VectorsHandlers;

import java.util.Random;

public class VectorOperations {

    public static Double dot(Double[] userVector, Double[] itemVector) {
        Double sum = 0.0;
        int latentFeatures = userVector.length;

        for (int k = 0; k < latentFeatures; k++) {
            sum += userVector[k] * itemVector[k];
        }
        return sum;
    }

    public static Double[] initializeVector(int latentFeature){
        //Initializing the vector with random number from gaussian with zero mean and 0.1 variance
        Double[] initializedVector =new Double[latentFeature];
        double variance = 0.1;
        Random random = new Random();
        for(int i=0;i<latentFeature;i++){
            //mean = 0 (+0)
            initializedVector[i] = random.nextGaussian()*variance;
        }

        return initializedVector;

    }

    public static Double[] initializeZeroVector(int latentFeature){
        //Initializing the vector with random number from gaussian with zero mean and 0.1 variance
        Double[] initializedVector =new Double[latentFeature];
        for(int i=0;i<latentFeature;i++){
            initializedVector[i] = 0.0;
        }

        return initializedVector;

    }

    public static Double[] incrementalAvgVector(Double[] lastAvg ,Double[] newVec, Double counter){

        int vecLen = lastAvg.length;
        Double[] resultVec = new Double[vecLen];
        for(int i=0;i<vecLen;i++){
            resultVec[i] = lastAvg[i] + ((newVec[i]-lastAvg[i])/counter);
        }

        return resultVec;

    }
}