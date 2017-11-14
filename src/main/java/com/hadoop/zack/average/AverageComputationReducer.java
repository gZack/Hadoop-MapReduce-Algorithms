package com.hadoop.zack.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageComputationReducer
        extends Reducer<Text, AverageComputationPair, Text, DoubleWritable> {

    private DoubleWritable doubleWritable = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<AverageComputationPair> values, Context context)
            throws IOException, InterruptedException {

        int totalCount = 0;
        int totalByteSize = 0;

        for(AverageComputationPair pair : values){
            totalCount += pair.getCount().get();
            totalByteSize += pair.getSize().get();
        }

        doubleWritable.set(totalByteSize/totalCount);

        context.write(key, doubleWritable);
    }
}
