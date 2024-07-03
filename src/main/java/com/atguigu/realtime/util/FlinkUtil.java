package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkUtil {
    public static MySqlSource<String> getMysqlSource(String databaseList, String tableList) {
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOSTNAME_GMALL)
                .port(Constant.MYSQL_PORT_GMALL)
                .databaseList(databaseList) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(databaseList+"."+tableList) // 设置捕获的表
                .username(Constant.MYSQL_USERNAME_GMALL)
                .password(Constant.MYSQL_PASSWORD_GMALL)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
    }

    ;

    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
