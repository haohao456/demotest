package com.haohao.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/15
 */
/*
 *  KEYIN：   reduce阶段输入的key的类型：Text
 *  VALUEIN： reduce阶段输入value类型：IntWritable
 *  KEYOUT:   reduce阶段输出的key类型：Text
 *  VALUEOUT：reduce阶段输出的value类型：IntWritable
 *
 * */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable>{

    private IntWritable outValue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        //atguigu,(1,1)
        for(IntWritable value:values){
            sum+=value.get();
        }

        outValue.set(sum);

        context.write(key,outValue);
    }
}
