package com.hadoop.zack.relativeFrequencies.pair;

import com.hadoop.zack.Consts;
import com.hadoop.zack.relativeFrequencies.ProductPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairRelativeCoocurrenceReducer
        extends Reducer<ProductPair,IntWritable,ProductPair,DoubleWritable> {

    private DoubleWritable totalCount = new DoubleWritable();

    private DoubleWritable doubleWritable = new DoubleWritable();

    protected void reduce(ProductPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        Integer count = 0;

        for(IntWritable value : values){

            count += value.get();

        }

        if(key.getNeighborId().toString().equals(Consts.ASTERIX)){

            totalCount.set(count);

        }else {

            doubleWritable.set((double)count/totalCount.get());

            context.write(key, doubleWritable);
        }
    }
}
