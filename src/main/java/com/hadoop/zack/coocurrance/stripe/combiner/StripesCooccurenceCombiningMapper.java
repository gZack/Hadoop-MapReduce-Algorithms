package com.hadoop.zack.coocurrance.stripe.combiner;

import com.hadoop.zack.Consts;
import com.hadoop.zack.CooccurenceService;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class StripesCooccurenceCombiningMapper
        extends Mapper<Object,Text,Text,MapWritable> {

    private MapWritable occurrenceMap = null;

    private IntWritable ONE = null;

    private static final int FLUSH_SIZE = 10000;

    private CooccurenceService service = new CooccurenceService();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        occurrenceMap = new MapWritable();

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] products = value.toString().split(Consts.EMPTY);

        if(products.length > 1){

            Text product;

            for(int i=0; i < products.length-1; i++){

                product = new Text(products[i]);

                MapWritable neighborMap = (MapWritable) occurrenceMap.get(product);

                if(neighborMap == null){

                    neighborMap = new MapWritable();

                    occurrenceMap.put(product,neighborMap);

                }

                List<String> neighbors = service.getNeighbor(product.toString(),i,products);

                for(String neighbor : neighbors){

                    Text neighborText = new Text(neighbor);

                    if(neighborMap.containsKey(neighborText)){

                        IntWritable count = (IntWritable) neighborMap.get(neighborText);

                        count.set(count.get()+1);

                    }else {

                        ONE = new IntWritable(1);

                        neighborMap.put(neighborText, ONE);
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flush(context,true);
    }

    private void flush(Mapper.Context context, boolean force)
            throws IOException, InterruptedException{

        MapWritable map = getOccurrenceMap();

        if(!force){

            int size = map.size();

            if(size < FLUSH_SIZE){

                return;

            }

        }

        Set<Writable> keys = map.keySet();

        for(Writable key : keys){

            context.write(key,map.get(key));

        }

        map.clear();
    }

    public MapWritable getOccurrenceMap() {
        return occurrenceMap;
    }
}
