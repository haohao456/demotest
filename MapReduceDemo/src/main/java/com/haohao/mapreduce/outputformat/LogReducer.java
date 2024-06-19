package com.haohao.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/19
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //防止有相同数据，丢数据
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
