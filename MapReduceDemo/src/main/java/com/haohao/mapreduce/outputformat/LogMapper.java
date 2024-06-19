package com.haohao.mapreduce.outputformat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author 郝浩
 * @date 2021/7/19
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //http://www.baidu.com
        //不做任何处理
        context.write(value,NullWritable.get());
    }
}
