package com.zetyun.hqbank.example.TopN;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * 窗口中的TopN的获取
 * @author zhaohaojie
 * @date 2024-09-02 23:18
 */
@Slf4j
public class SortProcessFunction extends KeyedProcessFunction<Long, Tuple3<Integer,Integer,Long>, String> {

    // 保存该windowEnd下的所有的用户财富值，并在生成一个endTime+1ms的定时器，即窗口关闭时，需要排序财富榜单并输出，
    HashMap<Long,List<Tuple3<Integer,Integer,Long>>> richesResultInWindow;
    Integer threshold ;

    public SortProcessFunction(Integer threshold) {
        System.out.println(" SortProcessFunction constructor method ");
        this.threshold = threshold;
        this.richesResultInWindow = new HashMap<>();
    }

    @Override
    public void processElement(Tuple3<Integer, Integer, Long> tuple3, Context context, Collector<String> collector) throws Exception {
        System.out.println(" SortProcessFunction processElement method ");
        Long windowEnd = tuple3.f2;
        if (richesResultInWindow.isEmpty()||!richesResultInWindow.containsKey(windowEnd)){
            System.out.println(" SortProcessFunction processElement add1 tuple3 = "+tuple3);
            List<Tuple3<Integer,Integer,Long>> list = new ArrayList<>();
            list.add(tuple3);

            richesResultInWindow.put(windowEnd,list);
        }else{
            System.out.println(" SortProcessFunction processElement add2 tuple3 = "+tuple3);

            List<Tuple3<Integer, Integer, Long>> tuple3s = richesResultInWindow.get(windowEnd);
            tuple3s.add(tuple3);
            richesResultInWindow.put(windowEnd,tuple3s);
        }
        System.out.println(" SortProcessFunction processElement list = "+richesResultInWindow);


        context.timerService().registerEventTimeTimer(windowEnd+1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 排序
        Long currentKey = ctx.getCurrentKey();
        System.out.println("on timer: key is "+currentKey);
        List<Tuple3<Integer, Integer, Long>> tuple3s = richesResultInWindow.get(currentKey);
        tuple3s.sort((o1, o2) -> o2.f1 - o1.f1);

        // 取topN
        StringBuilder sb = new StringBuilder();
        for (int i =0;i< this.threshold;i++){
            Tuple3<Integer, Integer, Long> richesAgg = tuple3s.get(i);
            sb.append("第").append(i+1).append("名：").append(richesAgg.f0).append("累计财富").append(richesAgg.f1).append(" 截止时间 ").append(richesAgg.f2);
        }
        out.collect(sb.toString());

    }
}

