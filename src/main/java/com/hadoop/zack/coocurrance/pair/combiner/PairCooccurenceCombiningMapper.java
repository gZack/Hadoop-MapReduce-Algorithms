package com.hadoop.zack.coocurrance.pair.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.coocurrance.ProductPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class PairCooccurenceCombiningMapper
        extends Mapper<Object,Text,ProductPair,IntWritable> {

    private Map<ProductPair,IntWritable> map = null;

    private static final int FLUSH_SIZE = 10000;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<ProductPair, IntWritable>();
    }

    IntWritable ONE = null;
    ProductPair productPair = null;

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{

        String[] products = value.toString().split(Consts.EMPTY);

        if(products.length > 1){
            String product;
            for(int i=0; i < products.length-1; i++){
                product = products[i];
                List<String> neighbors = getNeighbor(product,i,products);
                for(String neighbor : neighbors){
                    productPair = new ProductPair(product,neighbor);
                    if(map.containsKey(productPair)){
                        IntWritable count = map.get(productPair);
                        count.set(count.get() + 1);
                    } else {
                        ONE =  new IntWritable(1);
                        map.put(productPair, ONE);
                    }
                    flush(context,false);
                    //context.write(productPair, ONE);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flush(context,true);
    }

    private void flush(Mapper.Context context, boolean force)
            throws IOException, InterruptedException{

        Map<ProductPair,IntWritable> map = getMap();

        if(!force){
            int size = map.size();
            if(size < FLUSH_SIZE){
                return;
            }
        }

        Set<ProductPair> keys = map.keySet();
        for(ProductPair key : keys){
            context.write(key,map.get(key));
        }

        map.clear();
    }

    public Map<ProductPair, IntWritable> getMap() {
        return map;
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
