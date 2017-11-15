package com.hadoop.zack.part4.inmapper;

import com.hadoop.zack.Consts;
import com.hadoop.zack.ProductPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.hadoop.zack.CooccurenceService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HybridCombiningMapper extends Mapper<Object,Text,ProductPair,IntWritable> {

    private Map<ProductPair,IntWritable> map = null;

    private static final int FLUSH_SIZE = 10000;

    private IntWritable ONE = null;

    private ProductPair productPair = null;

    private CooccurenceService service = new CooccurenceService();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<>();
    }

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

                    if(map.containsKey(productPair)){

                        IntWritable count = map.get(productPair);

                        count.set(count.get() + 1);

                    } else {

                        ONE =  new IntWritable(1);

                        map.put(productPair, ONE);

                    }

                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        flush(context,true);

    }

    private void flush(Context context, boolean force)
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

}
