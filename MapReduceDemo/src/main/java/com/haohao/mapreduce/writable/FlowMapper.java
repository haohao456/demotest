package com.haohao.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/16
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200   7-3
        //2	13846544121	192.196.100.2			        264	     0	    200   6-3
        String s = value.toString();

        //2.切割成数组
        //"1,13736230513,192.196.100.1,www.atguigu.com,2481,24681,200"
        String[] split = s.split("\t");

        //3.抓取想要的数据
        //手机号
        //上行流量与下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        //4.封装
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        //5.写出
        context.write(outK, outV);
    }
}
