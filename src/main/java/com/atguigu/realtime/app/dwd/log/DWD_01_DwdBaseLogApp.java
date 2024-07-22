package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.SelfUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class DWD_01_DwdBaseLogApp extends BaseAppV1 {
    private final String START = "start";
    private final String PAGE = "page";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";

    public static void main(String[] args) {
        new DWD_01_DwdBaseLogApp().init(3101, 2, "DWD_01_DwdBaseLogApp", Constant.TOPIC_ODS_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. etl
        SingleOutputStreamOperator<JSONObject> etlEdStream = etlStream(stream);
        // 2. 纠正新老用户标识
        SingleOutputStreamOperator<JSONObject> correctedIsNewStream = correctIsNew(etlEdStream);
        // 3.数据分流
        HashMap<String, DataStream<JSONObject>> shuntedStream = shuntStream(correctedIsNewStream);
        // 4.写入Kafka
        writeToKafka(shuntedStream);
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> stream) {
        stream.get(START).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        stream.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        stream.get(ERR).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        stream.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        stream.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    /**
     * 主流：start
     * 侧输出流：display，action，page，err
     * 保留common
     *
     * @param stream
     * @return
     */
    private HashMap<String, DataStream<JSONObject>> shuntStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> displayTag = new OutputTag<>("displayTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> actionTag = new OutputTag<>("actionTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> pageTag = new OutputTag<>("pageTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> errTag = new OutputTag<>("errTag", TypeInformation.of(JSONObject.class));
        SingleOutputStreamOperator<JSONObject> processedStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (value.containsKey("start")) {
                            out.collect(value);
                        } else {
                            JSONArray displays = value.getJSONArray("displays");
                            JSONArray actions = value.getJSONArray("actions");
                            JSONObject common = value.getJSONObject("common");
                            JSONObject page = value.getJSONObject("page");
                            Long ts = value.getLong("ts");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.putAll(common);
                                    display.putAll(page);
                                    display.put("ts", ts);
                                    ctx.output(displayTag, display);
                                }
                                value.remove(displays);
                            }
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    action.putAll(common);
                                    action.putAll(page);
                                    ctx.output(actionTag, action);
                                }
                                value.remove(actions);
                            }
                            if (value.containsKey("err")) {
                                ctx.output(errTag, value);
                                value.remove("err");
                            }
                            if (page != null) {
                                ctx.output(pageTag, value);
                            }
                        }
                    }
                });
        HashMap<String, DataStream<JSONObject>> resultMap = new HashMap<>();
        resultMap.put(START, processedStream);
        resultMap.put(DISPLAY, processedStream.getSideOutput(displayTag));
        resultMap.put(ERR, processedStream.getSideOutput(errTag));
        resultMap.put(PAGE, processedStream.getSideOutput(pageTag));
        resultMap.put(ACTION, processedStream.getSideOutput(actionTag));
        return resultMap;
    }

    /**
     * 根据mid判断是否为新老用户
     * is_new = 1
     * ①状态为null，添加状态；②状态相同，不做改动；③状态不同，is_new改为0
     * is_new=0
     * ①状态为空，状态更新为昨天
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> correctIsNew(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .keyBy(x -> x.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> firstDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("firstDateStateDesc", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        Long ts = value.getLong("ts");
                        String todayDate = SelfUtil.timestampToDate(ts);
                        String firstDate = firstDateState.value();
                        if ("1".equals(isNew)) {
                            if (firstDate == null) {
                                firstDateState.update(todayDate);
                            } else if (!firstDate.equals(todayDate)) {
                                value.getJSONObject("common").put("is_new", "0");
                            }
                        } else {
                            if (firstDate == null) {
                                firstDateState.update(SelfUtil.timestampToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etlStream(DataStreamSource<String> stream) {
        return stream
                .filter(line -> {
                    try {
                        JSONObject.parseObject(line);
                    } catch (Exception e) {
                        System.out.println("非json格式" + line);
                        return false;
                    }
                    return true;
                })
                .map(JSONObject::parseObject);
    }
}
