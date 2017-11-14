package com.hadoop.zack.coocurrance.stripe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class ViewableMapWritable extends MapWritable {

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for(Writable key : keySet()){
            IntWritable value = (IntWritable) get(key);
            stringBuilder.append("(");
            stringBuilder.append((Text)key);
            stringBuilder.append(",");
            stringBuilder.append(value.get());
            stringBuilder.append(")");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
