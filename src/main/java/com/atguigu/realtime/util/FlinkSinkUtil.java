package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.SelfPhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSinkUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new SelfPhoenixSink();
    }

    public static SinkFunction getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        properties.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        return new FlinkKafkaProducer<String>(
//                "default",
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
