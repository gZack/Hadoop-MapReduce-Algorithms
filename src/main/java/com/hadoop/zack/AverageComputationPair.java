package com.hadoop.zack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageComputationPair
        implements Writable, WritableComparable<AverageComputationPair> {

    private IntWritable size;
    private IntWritable count;

    public AverageComputationPair(){
        set(new IntWritable(0),new IntWritable(0));
    }

    public AverageComputationPair(int size, int count){
        set(new IntWritable(size), new IntWritable(count));
    }

    public void set(int size, int count){
        this.size.set(size);
        this.count.set(count);
    }

    public void set(IntWritable size, IntWritable count){
        this.size = size;
        this.count = count;
    }

    public int compareTo(AverageComputationPair o) {
        int compareValue = this.size.compareTo(o.getSize());
        if(compareValue != 0){
            return compareValue;
        }
        return this.count.compareTo(o.getCount());
    }

    public static AverageComputationPair read(DataInput in) throws IOException{
        AverageComputationPair averagePair = new AverageComputationPair();
        averagePair.readFields(in);
        return averagePair;
    }

    public void write(DataOutput out) throws IOException {
        size.write(out);
        count.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        size.readFields(in);
        count.readFields(in);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) return true;
        if(obj == null || obj.getClass() != this.getClass()) return false;

        AverageComputationPair averageComputationPair = (AverageComputationPair) obj;
        if(!averageComputationPair.getSize().equals(this.getSize())) return false;
        if(!averageComputationPair.getCount().equals(this.getCount())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;

        result += 31 + size.hashCode();
        result += 31 + count.hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "AverageComputationPair(" +
                        "size=" + size + ", count=" + count + ")";
    }

    public IntWritable getSize() {
        return size;
    }

    public IntWritable getCount() {
        return count;
    }
}
