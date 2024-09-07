package com.zetyun.hqbank.example.keyby;

import com.zetyun.hqbank.bean.Student;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhaohaojie
 * @date 2024-08-24 11:45
 */
public class SensorAlertProcessFunction extends KeyedProcessFunction<String, Student, String>  {
    private transient ValueState<Double> lastReading;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("lastReading", Double.class);
        lastReading = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Student input, Context context, Collector<String> collector) throws Exception {


    }
}

