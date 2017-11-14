package com.hadoop.zack.coocurrance.stripe.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.coocurrance.stripe.StripesCooccurenceMapper;
import com.hadoop.zack.coocurrance.stripe.StripesCooccurenceReducer;
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
        job.setReducerClass(StripesCooccurenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(Consts.COOCURRENCE_INPUT_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Consts.STRIPES_COOCURRENCE_COMB_OUTPUT_DIR));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
