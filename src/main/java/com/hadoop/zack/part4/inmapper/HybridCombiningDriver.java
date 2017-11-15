package com.hadoop.zack.part4.inmapper;

import com.hadoop.zack.Consts;
import com.hadoop.zack.ProductPair;
import com.hadoop.zack.part4.HybridReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HybridCombiningDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Hybrid");
        job.setJarByClass(HybridCombiningDriver.class);
        job.setMapperClass(HybridCombiningMapper.class);
        job.setReducerClass(HybridReducer.class);
        job.setOutputKeyClass(ProductPair.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(Consts.COOCURRENCE_INPUT_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Consts.HYBRID_RELATIVE_COMB_OUTPUT_DIR));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
