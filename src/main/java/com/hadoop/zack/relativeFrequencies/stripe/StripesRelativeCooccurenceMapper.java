package com.hadoop.zack.relativeFrequencies.stripe;

import com.hadoop.zack.Consts;
import com.hadoop.zack.relativeFrequencies.CooccurenceService;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class StripesRelativeCooccurenceMapper
        extends Mapper<Object,Text,Text,ViewableMapWritable> {

    private ViewableMapWritable occurrenceMap = new ViewableMapWritable();

    private IntWritable ONE = null;

    private CooccurenceService service = new CooccurenceService();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] products = value.toString().split(Consts.EMPTY);

        if(products.length > 1){

            Text product;

            for(int i=0; i < products.length-1; i++){

                occurrenceMap.clear();

                product = new Text(products[i]);

                List<String> neighbors = service.getNeighbor(product.toString(),i,products);

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
}
