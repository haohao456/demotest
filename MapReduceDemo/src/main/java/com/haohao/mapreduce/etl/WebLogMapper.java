package com.haohao.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/20
 */
public class WebLogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();



        //3.ETL
        boolean result=parseLog(line,context);

        if(!result){
            return;
        }

        //4.写出
        context.write(value,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        //切割
        String[] split = line.split(" ");

        //判断一下日志的长度是否大于11
        if(split.length>11){
            return true;
        }else {
            return false;
        }
    }
}
