package com.hadoop.zack.relativeFrequencies.pair;

import com.hadoop.zack.Consts;
import com.hadoop.zack.relativeFrequencies.ProductPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairRelativeCoocurrenceDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Cooccurence - pair approach");
        job.setJarByClass(PairRelativeCoocurrenceDriver.class);
        job.setMapperClass(PairRelativeCooccurenceMapper.class);
        job.setReducerClass(PairRelativeCoocurrenceReducer.class);
        job.setOutputKeyClass(ProductPair.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(Consts.COOCURRENCE_INPUT_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Consts.PAIR_RELATIVE_COOCURRENCE_OUTPUT_DIR));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
