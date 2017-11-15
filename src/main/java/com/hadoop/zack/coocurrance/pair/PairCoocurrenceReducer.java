package com.hadoop.zack.coocurrance.pair;

import com.hadoop.zack.ProductPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairCoocurrenceReducer
        extends Reducer<ProductPair,IntWritable,ProductPair,IntWritable> {

    protected void reduce(ProductPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
