package com.hadoop.zack.wordCount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCountCombiningMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final int FLUSH_SIZE = 1000;
    private Map<String,Integer> map;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<String, Integer>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String,Integer> map = getMap();
        StringTokenizer itr = new StringTokenizer(value.toString());

        String token;
        while (itr.hasMoreTokens()) {
            token = itr.nextToken();
            if(map.containsKey(token)){
                int total = map.get(token) + 1;
                map.put(token, total);
            }else {
                map.put(token,1);
            }
        }
    }

    private void flush(Context context, boolean force) throws IOException, InterruptedException{

        Map<String,Integer> map = getMap();

        if(!force){
            int size = map.size();
            if(size < FLUSH_SIZE){
                return;
            }
        }

        Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Integer> entry = iterator.next();
            String key = entry.getKey();
            int total = entry.getValue();
            context.write(new Text(key), new IntWritable(total));
        }

        map.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flush(context,true);
    }

    public Map<String, Integer> getMap() {
        return map;
    }
}
