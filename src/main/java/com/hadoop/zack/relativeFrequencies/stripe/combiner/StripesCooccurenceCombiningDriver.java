package com.hadoop.zack.relativeFrequencies.stripe.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.relativeFrequencies.stripe.StripesRelativeCooccurenceReducer;
import com.hadoop.zack.relativeFrequencies.stripe.ViewableMapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesCooccurenceCombiningDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Cooccurence - stripes in mapper approach");
        job.setJarByClass(StripesCooccurenceCombiningDriver.class);
        job.setMapperClass(StripesCooccurenceCombiningMapper.class);
        job.setReducerClass(StripesRelativeCooccurenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ViewableMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(Consts.COOCURRENCE_INPUT_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Consts.STRIPES_RELATIVE_COOCURRENCE_COMB_OUTPUT_DIR));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
