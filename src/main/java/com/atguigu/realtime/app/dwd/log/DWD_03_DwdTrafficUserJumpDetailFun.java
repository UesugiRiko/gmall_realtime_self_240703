package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DWD_03_DwdTrafficUserJumpDetailFun extends BaseAppV1 {
    public static void main(String[] args) {
        new DWD_03_DwdTrafficUserJumpDetailFun().init(3103, 2, "DWD_03_DwdTrafficUserJumpDetailFun", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

//        stream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":18000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":16000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":30000} ",
//                        "{\"common\":{\"mid\":\"103\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"103\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":26000} "
//                );

        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        // 1.定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("enter1")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("enter2")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .within(Time.seconds(5));
        // 2.模式应用到流上
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        // 3.获取数据
        SingleOutputStreamOperator<String> normal = ps
                .select(new OutputTag<String>("timeout") {
                        },
                        new PatternTimeoutFunction<JSONObject, String>() {
                            // 超时的数据
                            @Override
                            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.get("enter1").get(0).toJSONString();
                            }
                        },
                        new PatternSelectFunction<JSONObject, String>() {
                            // 主页-> 主页的数据
                            @Override
                            public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                                return pattern.get("enter1").get(0).toJSONString();
                            }
                        }
                );

//        normal.getSideOutput(new OutputTag<String>("timeout") {
//        }).print("tm");
//        normal.print("nor");

        normal
                .getSideOutput(new OutputTag<String>("timeout") {
                })
                .union(normal)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UJ_DETAIL));
//                .print();
    }
}
