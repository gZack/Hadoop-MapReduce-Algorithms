package com.hadoop.zack.coocurrance.stripe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Set;

public class StripesCooccurenceReducer
        extends Reducer<Text,MapWritable,Text,ViewableMapWritable> {

    private ViewableMapWritable result = new ViewableMapWritable();

    protected void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        result.clear();

        for(MapWritable value : values){
            pairWiseSum(value);
        }

        context.write(key,result);
    }

    private void pairWiseSum(MapWritable value){
        Set<Writable> keys = value.keySet();
        for(Writable key : keys){
            IntWritable count = (IntWritable) value.get(key);
            if(result.containsKey(key)){
                IntWritable totalCount = (IntWritable) result.get(key);
                totalCount.set(totalCount.get() + count.get());
            } else {
              result.put(key,count);
            }
        }
    }

}
