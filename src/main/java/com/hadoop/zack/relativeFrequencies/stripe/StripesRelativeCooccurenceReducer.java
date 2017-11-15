package com.hadoop.zack.relativeFrequencies.stripe;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

public class StripesRelativeCooccurenceReducer
        extends Reducer<Text,MapWritable,Text,ViewableMapWritable> {

    private ViewableMapWritable result = new ViewableMapWritable();

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        result.clear();

        for(MapWritable value : values){

            pairWiseSum(value);

        }

        ViewableMapWritable average = calculateAverage(result);

        context.write(key,average);

    }

    private ViewableMapWritable calculateAverage(ViewableMapWritable value){

        int size = result.size();

        ViewableMapWritable average = new ViewableMapWritable();

        for(Writable key : value.keySet()){

            IntWritable count = (IntWritable) value.get(key);

            average.put(key,new DoubleWritable((double) count.get() / size));

        }

        return average;

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
