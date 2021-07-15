package com.djt.flink.job;

import cn.hutool.setting.Setting;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Flink Kafka Source
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-15
 */
public class KafkaStreamFlinkJob extends AbsFlinkJob {

    public KafkaStreamFlinkJob(String jobName, Setting setting) {
        super(jobName, setting);
    }

    @Override
    protected void executeAction() {
        String topic = setting.get("kafka", "topic");
        Properties kafkaProps = getKafkaProps();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        exeEnv.enableCheckpointing(5000);
        DataStreamSource<String> kafkaStreamSource = exeEnv.addSource(kafkaConsumer);
        //kafkaStreamSource.print().setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv);
        tableEnv.createTemporaryView("table1", kafkaStreamSource);

    }
}
