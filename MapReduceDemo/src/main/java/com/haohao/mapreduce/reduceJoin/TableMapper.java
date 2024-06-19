package com.haohao.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/20
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String filename;   //因为在其他方法中会用到filename，因此要设置为全局变量
    private Text outK=new Text();
    private TableBean outV=new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化 order pd
        //对于一个文件获取一次filename，如果放在map中，会每读取一行获取一下filename,效率反而会降低
        FileSplit split = (FileSplit) context.getInputSplit();

        filename = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line=value.toString();

        //2.判断是哪个文件
        if(filename.contains("order")){   //处理的是订单表

            String[] split = line.split("\t");

            //封装k,v
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");

        }else{//处理的是商品表

            String[] split = line.split("\t");

            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        //写出
        context.write(outK,outV);
    }
}
