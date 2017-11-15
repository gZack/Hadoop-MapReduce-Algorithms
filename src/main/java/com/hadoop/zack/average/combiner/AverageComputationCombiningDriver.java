package com.hadoop.zack.average.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.AverageComputationPair;
import com.hadoop.zack.average.AverageComputationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageComputationCombiningDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Average calculation");
        job.setJarByClass(AverageComputationCombiningDriver.class);
        job.setMapperClass(AverageComputationCombiningMapper.class);
        job.setReducerClass(AverageComputationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AverageComputationPair.class);
        FileInputFormat.addInputPath(job, new Path(Consts.AVERAGE_INPUT_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Consts.AVERAGE_COMB_OUTPUT_DIR));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
