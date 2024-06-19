package com.haohao.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/19
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream hhout;

    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        //创建两条流
        try {
            //获取文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());

            //用文件系统对象创建两个输出流对应不同的目录
            hhout = fs.create(new Path("D:\\shangguigu\\hadoop3.0\\资料\\资料\\_output\\hh.log"));

            otherOut = fs.create(new Path("D:\\shangguigu\\hadoop3.0\\资料\\资料\\_output\\other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        String log=key.toString();

        //根据一行的 log 数据是否包含 atguigu,判断两条输出流输出的内容
        //具体写
         if(log.contains("atguigu")){
             hhout.writeBytes(log+"\n");
         }else {
             otherOut.writeBytes(log="\n");
         }




    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        //关流

        IOUtils.closeStream(hhout);
        IOUtils.closeStream(otherOut);
    }
}
