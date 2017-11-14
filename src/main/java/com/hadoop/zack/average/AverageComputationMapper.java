package com.hadoop.zack.average;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageComputationMapper extends Mapper<Object, Text, Text, AverageComputationPair>{

    private Text ipKey = new Text();
    private AverageComputationPair pair = null;
    InetAddressValidator ipValidator = new InetAddressValidator();
    private static final String EMPTY = "\\s+";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String byteSizeStr = value.toString().substring(value.toString().lastIndexOf(EMPTY)).trim();
        String ip = value.toString().substring(0,value.toString().indexOf(EMPTY));

        if(ipValidator.isValid(ip)){

            ipKey = new Text(ip);
            Integer byteSize = null;

            try {
                byteSize = Integer.parseInt(byteSizeStr);
                pair = new AverageComputationPair(byteSize,1);
                context.write(ipKey,pair);
            } catch (NumberFormatException e){
                //do nothing - skip to the next record
            }
        }

    }

}
