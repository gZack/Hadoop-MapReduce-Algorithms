package com.hadoop.zack.relativeFrequencies.pair;

import com.hadoop.zack.Consts;
import com.hadoop.zack.relativeFrequencies.CooccurenceService;
import com.hadoop.zack.relativeFrequencies.ProductPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class PairRelativeCooccurenceMapper
        extends Mapper<Object,Text,ProductPair,IntWritable> {

    IntWritable ONE = null;

    ProductPair productPair = null;

    ProductPair counterPair = null;

    private CooccurenceService service = new CooccurenceService();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{

        String[] products = value.toString().split(Consts.EMPTY);

        if(products.length > 1){

            String product;
            for(int i=0; i < products.length-1; i++){
                product = products[i];
                List<String> neighbors = service.getNeighbor(product,i,products);
                for(String neighbor : neighbors){
                    productPair = new ProductPair(product,neighbor);
                    counterPair = new ProductPair(product, "*");
                    ONE = new IntWritable(1);
                    context.write(productPair, ONE);
                    context.write(counterPair, ONE);
                }
            }
        }
    }

}
