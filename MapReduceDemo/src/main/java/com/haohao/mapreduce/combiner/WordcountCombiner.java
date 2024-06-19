package com.haohao.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/19
 */
public class WordcountCombiner extends Reducer<Text, IntWritable,Text,IntWritable>{

    private IntWritable outV=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;

        for(IntWritable value:values){
            sum+=value.get();
        }

        outV.set(sum);

        context.write(key,outV);
    }
}
