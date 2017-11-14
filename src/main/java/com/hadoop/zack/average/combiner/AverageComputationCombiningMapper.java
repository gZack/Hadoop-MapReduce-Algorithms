package com.hadoop.zack.average.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.average.AverageComputationPair;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class AverageComputationCombiningMapper
        extends Mapper<Object, Text, Text, AverageComputationPair>{

    //associative array
    private Map<String, AverageComputationPair> map = null;

    private AverageComputationPair pair = null;
    private InetAddressValidator ipValidator = new InetAddressValidator();

    private static final int FLUSH_SIZE = 10000;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<String, AverageComputationPair>();
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String byteSizeStr = value.toString().substring(value.toString().lastIndexOf(Consts.EMPTY)).trim();
        String ipStr = value.toString().substring(0,value.toString().indexOf(Consts.EMPTY));

        if(ipValidator.isValid(ipStr)){
            Integer byteSize = null;
            try {
                byteSize = Integer.parseInt(byteSizeStr);
                if(map.containsKey(ipStr)){
                    AverageComputationPair existingPair = map.get(ipStr);
                    existingPair.set(existingPair.getSize().get() + byteSize,
                            existingPair.getCount().get() + 1);
                }else {
                    pair = new AverageComputationPair(byteSize,1);
                    map.put(ipStr,pair);
                }
                //context.write(ipKey,pair);
            } catch (NumberFormatException e){
                //do nothing - skip to the next record
            }
            flush(context,false);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flush(context,true);
    }

    private void flush(Mapper.Context context, boolean force)
            throws IOException, InterruptedException{

        Map<String,AverageComputationPair> map = getMap();

        if(!force){
            int size = map.size();
            if(size < FLUSH_SIZE){
                return;
            }
        }

        Set<String> keys = map.keySet();
        Text text = new Text();
        for(String key : keys){
            text.set(key);
            context.write(text,map.get(key));
        }

        map.clear();
    }

    public Map<String, AverageComputationPair> getMap() {
        return map;
    }
}
