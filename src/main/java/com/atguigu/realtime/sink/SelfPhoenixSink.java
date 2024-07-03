package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDsUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class SelfPhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDsUtil.getDruidDataSource();
    }

    @Override
    public void close() throws Exception {
        druidDataSource.close();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        sendDataToPhoenix(value);
    }

    private void sendDataToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        DruidPooledConnection connection = druidDataSource.getConnection();
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(") values (")
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");
        System.out.println("sql语句" + sql);
        PreparedStatement ps = connection.prepareStatement(sql.toString());
        String[] tpCol = tp.getSinkColumns().split(",");
        for (int i = 0; i < tpCol.length; i++) {
            String blank = data.get(tpCol[i]) == null ? null : data.get(tpCol[i]).toString();
            ps.setString(i + 1, blank);
        }
        ps.execute();
        connection.commit();
        ps.close();
        connection.close();
    }
}
