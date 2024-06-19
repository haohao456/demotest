package com.haohao.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 郝浩
 * @date 2021/7/19
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //text是手机号

        //获取手机号前三位 prePhone
        String phone=text.toString();

        String prePhone = phone.substring(0, 3);//包左不包右

        //定义一个分区号变量 partition,根据 prePhone 设置分区号
        int partition;

        switch (prePhone) {
            case "136":           //常量放在前面比较好，就不用做空指针的操作了：相当于136调用了equals方法
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
                break;
        }

        //最后返回分区号 partition
        return partition;
    }
}
