package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.SelfUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// 依旧不行，还是得用单纯的状态处理

// TODO: 2024/7/4
public class DWD_02_DwdTrafficUniqueVisitDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new DWD_02_DwdTrafficUniqueVisitDetail().init(3102, 2, "DWD_02_DwdTrafficUniqueVisitDetail", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        /*
        {
            "common":{
                "ar":"310000","uid":"48","os":"Android 10.0","ch":"wandoujia","is_new":"0","md":"Huawei Mate 30","mid":"mid_3","vc":"v2.1.134","ba":"Huawei"
            },
            "page":{
                "page_id":"good_detail","item":"10","during_time":12017,"item_type":"sku_id","last_page_id":"good_list","source_type":"activity"
            },
            "ts":1719303614000
        }
         */
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
//
//                    private ValueState<String> firstVisitDateState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDatetimeStateDesc", String.class));
//                    }
//
//                    @Override
//                    public void process(String s, ProcessWindowFunction<JSONObject, String, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
//                        String firstVisitDate = firstVisitDateState.value();
//                        String today = SelfUtil.timestampToDate(context.window().getStart());
//                        if (firstVisitDate == null || !firstVisitDate.equals(today)) {
//                            firstVisitDateState.update(today);
//                            List<JSONObject> list = SelfUtil.iterableToList(elements);
//                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
//                            out.collect(min.toJSONString());
//                        }
//                    }
//                })
//                .print();
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    private ValueState<String> visitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateStateDesc", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        String firstVisitDate = visitDateState.value();
                        String today = SelfUtil.timestampToDate(value.getLong("ts"));
                        if (!today.equals(firstVisitDate)) {
                            out.collect(value.toJSONString());
                            visitDateState.update(today);
                        }

                    }
                })
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV_DETAIL));

    }
}
