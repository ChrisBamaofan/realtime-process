package com.zetyun.hqbank.example.State;

import com.zetyun.hqbank.bean.ClickEvents;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

/**
 * @author zhaohaojie
 * @date 2024-09-04 12:46
 */
public class ListStateExample extends RichFlatMapFunction<ClickEvents,HashMap<String, List<String>>> {
    private transient ListState<String> counter;


    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<String>("ListStateExample",String.class);
        counter = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(ClickEvents clickEvents, Collector<HashMap<String, List<String>>> collector) throws Exception {


    }
}

