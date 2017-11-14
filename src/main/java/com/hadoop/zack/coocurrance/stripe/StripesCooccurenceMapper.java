package com.hadoop.zack.coocurrance.stripe;

import com.hadoop.zack.Consts;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StripesCooccurenceMapper
        extends Mapper<Object,Text,Text,MapWritable> {

    private MapWritable occurrenceMap = new MapWritable();
    IntWritable ONE = null;

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] products = value.toString().split(Consts.EMPTY);

        if(products.length > 1){
            Text product = null;

            for(int i=0; i < products.length-1; i++){
                occurrenceMap.clear();
                product = new Text(products[i]);

                List<String> neighbors = getNeighbor(product.toString(),i,products);
                for(String neighbor : neighbors){
                    Text neighborText = new Text(neighbor);
                    if(occurrenceMap.containsKey(neighborText)){
                        IntWritable count = (IntWritable) occurrenceMap.get(neighborText);
                        count.set(count.get()+1);
                    }else {
                        ONE = new IntWritable(1);
                        occurrenceMap.put(neighborText, ONE);
                    }
                }
                context.write(product, occurrenceMap);
            }
        }
    }


    private List<String> getNeighbor(String productId, int beginIndex, String[] products){
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
