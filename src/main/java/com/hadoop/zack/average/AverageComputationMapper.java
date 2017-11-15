package com.hadoop.zack.average;

import com.hadoop.zack.Consts;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageComputationMapper extends Mapper<Object, Text, Text, AverageComputationPair>{

    private Text ipKey = new Text();

    private AverageComputationPair pair = null;

    private InetAddressValidator ipValidator = new InetAddressValidator();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] logRecords = splitLogRecord(value.toString());

       //value.toString().substring(value.toString().lastIndexOf(Consts.EMPTY)).trim();

        String ip = logRecords.length > 0 ? logRecords[0] : ""; //value.toString().substring(0,value.toString().indexOf(Consts.EMPTY));

        if(ipValidator.isValid(ip)){

            ipKey = new Text(ip);

            int byteSize;

            try {

                byteSize = logRecords.length > 0 ?
                        Integer.valueOf(logRecords[logRecords.length - 1]) : 0;

                pair = new AverageComputationPair(byteSize,1);

                context.write(ipKey,pair);

            } catch (NumberFormatException e){
                //do nothing - skip to the next record
            }
        }

    }

    private String[] splitLogRecord(String logRecord){
        return logRecord.split(Consts.EMPTY);
    }

}
