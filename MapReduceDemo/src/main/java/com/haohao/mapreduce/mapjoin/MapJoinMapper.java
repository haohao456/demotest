package com.haohao.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author 郝浩
 * @date 2021/7/20
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String,String> pdmap=new HashMap<>();
    private Text outK=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存的文件，并把文件内容封装在集合pd.txt
        URI[] cacheFiles = context.getCacheFiles();

        FileSystem fs=FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        //从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line=reader.readLine())){
            //切割
            String[] fields=line.split("\t");

            //赋值
            pdmap.put(fields[0],fields[1]);
        }
        //关流
        IOUtils.closeStream(reader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //处理order.txt
        String line = value.toString();
        String[] split = line.split("\t");

        //获取pid
        String pname = pdmap.get(split[1]);

        //获取订单id和订单数量
        //封装
        outK.set(split[0]+"\t"+pname+"\t"+split[2]);

        context.write(outK,NullWritable.get());
    }
}
