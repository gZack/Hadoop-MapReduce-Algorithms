package com.hadoop.zack.part4;

import com.hadoop.zack.ProductPair;
import com.hadoop.zack.ViewableMapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HybridReducer
        extends Reducer<ProductPair,IntWritable,Text,ViewableMapWritable> {

    private ViewableMapWritable result = null;

    private Text prevLeft;

    private IntWritable total;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        result = new ViewableMapWritable();

    }

    @Override
    protected void reduce(ProductPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        if(prevLeft != null
                && !key.getProductId().toString().equals(prevLeft.toString())){

            relativeFrequency(result);

            context.write(prevLeft,result);

            result.clear();

        }

        total = new IntWritable(0);

        for(IntWritable value : values){

            total.set(total.get() + value.get());

        }

        result.put(new Text(key.getNeighborId().toString()), total);

        prevLeft = new Text(key.getProductId().toString());

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        relativeFrequency(result);

        context.write(prevLeft,result);

    }

    private void relativeFrequency(ViewableMapWritable map){

        int total = map.values().stream()
                        .map(writable -> ((IntWritable)writable).get())
                        .reduce(0,(x,y) -> x+y);

        DoubleWritable average;

        int value;

        for(Writable key : map.keySet()){

            value = ((IntWritable)map.get(key)).get();

            average = new DoubleWritable(round((double) value/total));

            map.put(key,average);
        }

    }

    private double round(double value){
        return Math.round(value * 100D) / 100D;
    }

}
