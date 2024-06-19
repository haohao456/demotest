package com.haohao.mapreduce.combineTextInputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/15
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        //2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class:右4个文件，会产生4个maptask
        job.setInputFormatClass(CombineTextInputFormat.class);   //这里设置的是CombineTextInputFormat

        //虚拟存储切片最大值设置 4m
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);       //产生3个maptask
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);   //产生1个maptask


        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\shangguigu\\hadoop3.0\\资料\\资料\\11_input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\shangguigu\\hadoop3.0\\资料\\资料\\_output\\outputCombine2"));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
