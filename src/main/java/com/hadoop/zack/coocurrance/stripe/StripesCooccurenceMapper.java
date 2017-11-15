package com.hadoop.zack.coocurrance.stripe;

import com.hadoop.zack.Consts;
import com.hadoop.zack.CooccurenceService;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class StripesCooccurenceMapper
        extends Mapper<Object,Text,Text,MapWritable> {

    private MapWritable occurrenceMap = new MapWritable();

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
