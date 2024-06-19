package com.haohao.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/16
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outv = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //1.遍历集合
        long totalup = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldown += value.getDownFlow();
        }

        //2.封装outk,outv
        outv.setUpFlow(totalup);
        outv.setDownFlow(totaldown);
        outv.setSumFlow();

        //3.写出
        context.write(key, outv);
    }
}
