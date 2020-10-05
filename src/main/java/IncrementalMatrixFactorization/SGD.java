package IncrementalMatrixFactorization;

import Type_classes.userItem;
import VectorsHandlers.VectorOperations;

public class SGD{

    private userItem userItemVectors = new userItem();



    public userItem update_isgd(Double[] input_userVector, Double[] input_itemVector, Double mu, Double lambda) {

        //positive feedback only so the error is subtracted from one(implicit feedback)
        //userItemVectors userItem = new userItemVectors();
        Double[] iVec = input_itemVector.clone();
        Double[] uVec = input_userVector.clone();


        Double error = 1 - (VectorOperations.dot(uVec, iVec));

        for (int k = 0; k < input_itemVector.length; k++) {
            double userFeature = uVec[k];
            double itemFeature = iVec[k];
            uVec[k] += mu * (error * itemFeature - lambda * userFeature);
            iVec[k] += mu * (error * uVec[k] - lambda * itemFeature);
        }

        userItemVectors.userVector = uVec;
        userItemVectors.itemVector = iVec;

        return userItemVectors;
    }

}