package com.haohao.mapreduce.partitioner2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 郝浩
 * @date 2021/7/16
 */

/*
*  1.定义类实现writable接口
*  2.重写序列化和反序列化
*  3.重写空参构造
*  4.toString()方法
*
* */
//1 继承 Writable 接口
public class FlowBean implements Writable {


    private long upFlow;  //上行流量
    private long downFlow; //下行流量
    private long sumFlow;  //总流量

    //2.空参构造
    public FlowBean() {
    }

    //3 提供三个参数的 getter 和 setter 方法
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
    }

    //4 实现序列化和反序列化方法,注意顺序一定要保持一致
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sumFlow=in.readLong();
    }

    //5 重写 ToString
    @Override
    public String toString() {
        return  upFlow +"\t" + downFlow +"\t" + sumFlow;
    }
}
