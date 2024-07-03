package com.atguigu.realtime.app;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAppV1 {
    public void init(int port, int parallelism, String jobName, String topic) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        env.enableCheckpointing(3000);
        env.enableCheckpointing().setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(20000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall_test/" + jobName);

        DataStreamSource<String> stream = env.fromSource(FlinkUtil.getKafkaSource(jobName, topic), WatermarkStrategy.noWatermarks(), "Kafka Source");

        handle(env, stream);

        try {
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
}
