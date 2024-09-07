package com.zetyun.hqbank.example.TopN;

import com.zetyun.hqbank.bean.Riches;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author zhaohaojie
 * @date 2024-09-02 20:53
 */
public class RichesAggregation implements AggregateFunction<Riches,Integer,Integer>  {


    @Override
    public Integer createAccumulator() {
        System.out.println(" initialize accumulator "+0);
        return 0;
    }

    @Override
    public Integer add(Riches riches, Integer integer) {
        System.out.println("rich = "+riches.getRiches()+"add ="+integer);
        return riches.getRiches()+integer;
    }

    @Override
    public Integer getResult(Integer integer) {
        System.out.println("getResult="+integer);
        return integer;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        System.out.println("merge="+integer+" acc1="+acc1);
        return integer+acc1;
    }
}

