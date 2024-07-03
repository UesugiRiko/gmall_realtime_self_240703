package com.atguigu.realtime.app.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkJdbcUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.FlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseAppV1 {


    public static void main(String[] args) {
        new DimApp().init(3000, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1.etl
        SingleOutputStreamOperator<JSONObject> etlEdStream = etl(stream);
        // 2.读取配置信息
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3.数据流和广播流connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connectDataBC(etlEdStream, tpStream);
        // 4.剔除数据流中的无关数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultDimStream = filterColumns(dataTpStream);
        // 5.dim层数据写入phoenix
        writeToPhoenix(resultDimStream);
    }

    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getPhoenixSink());
    }

    // 只保留phoenix表相关的字段和自定义的type字段（只留存dim层的数据字段）
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        return stream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        JSONObject data = value.f0;
                        List<String> columns = Arrays.asList(value.f1.getSinkColumns().split(","));
                        data.keySet().removeIf(key -> !columns.contains(key) && !"ods_db_type".equals(key));
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDataBC(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> tpBroadcastStateDesc;
        // 0.根据配置信息建表，广播前建表，广播后会导致重复建表
        tpStream = tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {

            private Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = FlinkJdbcUtil.getPhoenixConnection();
            }

            @Override
            public void close() throws Exception {
                FlinkJdbcUtil.closePhoenixConnection(conn);
            }

            // 建表
            @Override
            public TableProcess map(TableProcess value) throws Exception {
                if (conn.isClosed()) {
                    conn = FlinkJdbcUtil.getPhoenixConnection();
                }
                StringBuilder sql = new StringBuilder();
                sql
                        .append("create table if not exists ")
                        .append(value.getSinkTable())
                        .append("(")
                        .append(value.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                        .append(", constraint pk primary key(")
                        .append(value.getSinkPk() == null ? "id" : value.getSinkPk())
                        .append("))")
                        .append(value.getSinkExtend() == null ? "" : value.getSinkExtend());
                System.out.println("phoenix建表语句: " + sql);
                PreparedStatement ps = conn.prepareStatement(sql.toString());
                ps.execute();
                ps.close();
                return value;
            }
        });
        // 1.配置信息流做成广播流
        tpBroadcastStateDesc = new MapStateDescriptor<>("tpBcState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(tpBroadcastStateDesc);
        // 2.数据流connect广播流
        return dataStream
                .connect(tpBcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    // 4.数据流处理时，获取对应的广播状态中的配置信息
                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> bcState = ctx.getBroadcastState(tpBroadcastStateDesc);
                        if (bcState.get(value.getString("table")) != null) {
                            JSONObject jsonObject = value.getJSONObject("data");
                            jsonObject.put("ods_db_type", value.getString("type"));
                            out.collect(Tuple2.of(jsonObject, bcState.get(value.getString("table"))));
                        }
                    }

                    // 3.配置信息写入广播状态
                    @Override
                    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        BroadcastState<String, TableProcess> bcState = ctx.getBroadcastState(tpBroadcastStateDesc);
                        bcState.put(value.getSourceTable(), value);
                    }
                });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        return env
                .fromSource(FlinkUtil.getMysqlSource("gmall_config", "table_process"), WatermarkStrategy.noWatermarks(), "Mysql Source")
                .map(json -> JSON.parseObject(json).getObject("after", TableProcess.class));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(json -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(json.replaceAll("bootstrap-", ""));
                        return ("insert".equals(jsonObject.getString("type")) || "update".equals(jsonObject.getString("type")))
                                && "gmall".equals(jsonObject.getString("database"))
                                && jsonObject.getString("data") != null
                                && jsonObject.getString("data").length() > 2;
                    } catch (Exception e) {
                        System.out.println("非json数据" + json);
                        return false;
                    }
                })
                .map(JSON::parseObject);
    }
}
