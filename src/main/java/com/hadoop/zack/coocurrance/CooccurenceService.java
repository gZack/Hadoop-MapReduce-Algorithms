package com.hadoop.zack.coocurrance;

import java.util.ArrayList;
import java.util.List;

public class CooccurenceService {
    public List<String> getNeighbor(String productId, int beginIndex, String[] products){
        List<String> neighbors = new ArrayList<String>();
        beginIndex++;
        String product;
        for(;beginIndex < products.length; beginIndex++){
            product = products[beginIndex];
            if(product.equals(productId)){
                break;
            }
            neighbors.add(product);
        }

        return neighbors;
    }
}
