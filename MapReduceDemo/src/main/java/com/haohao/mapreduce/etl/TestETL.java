package com.haohao.mapreduce.etl;

/**
 * @author 郝浩
 * @date 2021/7/20
 */
public class TestETL {

    public static void main(String[] args) {

        String check="^((13[0-9])|(14[0-9])|(15[0-9])|(17[0-9])|(18[0-9]))\\d{8}$";

        String phone="15591020233";

        System.out.println(phone.matches(check));
    }
}
