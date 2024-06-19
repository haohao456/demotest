package com.haohao.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author 郝浩
 * @date 2021/7/20
 */
public class TableReduce extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //01 1001 1 order
        //01 1004 4 order
        //01 小米 pd

        ArrayList<TableBean> orderBeans = new ArrayList<>();

        TableBean pdBean = new TableBean();

        //循环遍历
        for(TableBean value:values){

            if("order".equals(value.getFlag())){   //订单表

                //创建一个临时 TableBean 对象接收 value
                TableBean tmptableBean =new TableBean();

                try {
                    BeanUtils.copyProperties(tmptableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderBeans.add(tmptableBean);


            }else{//商品表

                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }
        }

        //循环遍历orderBeans,复制pdname
        for(TableBean orderBean:orderBeans){

            orderBean.setPname(pdBean.getPname());

            context.write(orderBean,NullWritable.get());
        }


    }
}
