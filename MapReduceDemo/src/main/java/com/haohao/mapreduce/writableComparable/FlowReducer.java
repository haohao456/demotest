package com.haohao.mapreduce.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/16
 */
public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text value:values){

            context.write(value,key);
    }
}
}
