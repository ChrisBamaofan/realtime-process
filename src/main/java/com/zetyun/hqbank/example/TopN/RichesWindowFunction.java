package com.zetyun.hqbank.example.TopN;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * in: 该user 某时刻的财富值
 * out: user 某时刻的 累计 财富值
 * key: user id
 * window: 窗口函数
 * @author zhaohaojie
 * @date 2024-09-02 22:20
 */
public class RichesWindowFunction extends ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>,Integer, TimeWindow> {

    /**
     *
     * @param key
     * @param context
     * @param iterable
     * @param collector 里面是聚合完的数据的结果，即这个user在这个窗口里的财富值总和
     * @throws Exception
     */
    @Override
    public void process(Integer key, Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
        long end = context.window().getEnd();
        Integer riches = iterable.iterator().next();
        Tuple3<Integer, Integer, Long> tuple3 = new Tuple3<>();
        tuple3.f0= key;
        tuple3.f1= riches;
        tuple3.f2 = end;
        collector.collect(tuple3);
    }
}

