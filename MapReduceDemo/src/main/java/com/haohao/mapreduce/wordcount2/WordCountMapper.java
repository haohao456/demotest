package com.haohao.mapreduce.wordcount2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/15
 */

/*
*  KEYIN：   map阶段输入的key的类型：LongWritable
*  VALUEIN： map阶段输入value类型：Text
*  KEYOUT:   map阶段输出的key类型：Text
*  VALUEOUT：map阶段输出的value类型：IntWritable
*
* */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outKey=new Text();
    private IntWritable outvalue=new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        //atguigu atguigu
        String s = value.toString();

        //2.切割
        //atguigu
        //atguigu
        String[] words= s.split(" ");

        //3.循环遍历
        for(String word:words){
            //封装outKey
            outKey.set(word);

            context.write(outKey,outvalue);
        }

    }
}
