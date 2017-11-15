package com.hadoop.zack.relativeFrequencies.stripe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ViewableMapWritable extends MapWritable {

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for(Writable key : keySet()){
            Writable value = get(key);
            stringBuilder.append("(");
            stringBuilder.append(key);
            stringBuilder.append(",");
            stringBuilder.append(value);
            stringBuilder.append(")");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
