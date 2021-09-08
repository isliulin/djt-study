package com.djt.utils;

import cn.hutool.setting.dialect.Props;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 配置参数名常量
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-20
 */
public interface ConfigConstants {

    /**
     * 基本配置
     */
    Props PROPS = Props.getProp("config.properties", StandardCharsets.UTF_8);

    /**
     * yyyyMMdd
     */
    DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 是否输出日志
     */
    String FLINK_PRINT_LOG = "flink.print.log";

    /**
     * 事件消息 消费topic
     */
    String TOPIC_EVENT = "topic.event";

    /**
     * 统计结果 生产topic
     */
    String TOPIC_STAT = "topic.stat";

    /**
     * HBase 统计结果表
     */
    String HBASE_TABLE_STAT = "hbase.table.stat";

    /**
     * HBase写入刷新 最大字节数
     */
    String HBASE_BUFFER_FLUSH_MAX_IN_BYTES = "hbase.buffer.flush.max.in.bytes";

    /**
     * HBase写入刷新 最大请求数
     */
    String HBASE_BUFFER_FLUSH_MAX_MUTATIONS = "hbase.buffer.flush.max.mutations";

    /**
     * HBase写入刷新 最大等待时间
     */
    String HBASE_BUFFER_FLUSH_INTERVAL_MILLIS = "hbase.buffer.flush.interval.millis";

    /**
     * Flink 算子默认并行度
     */
    String FLINK_ENV_PARALLELISM = "flink.env.parallelism";

    /**
     * Flink kafka sourc并行度
     */
    String FLINK_SOURCE_KAFKA_PARALLELISM = "flink.source.kafka.parallelism";

    /**
     * Flink HBase sink并行度
     */
    String FLINK_SINK_HBASE_PARALLELISM = "flink.sink.hbase.parallelism";

    /**
     * Flink 聚合计算并行度
     */
    String FLINK_AGGREGATE_PARALLELISM = "flink.aggregate.parallelism";


    /**
     * flink.watermark.interval
     */
    String FLINK_WATERMARK_INTERVAL = "flink.watermark.interval";

    /**
     * flink.checkpoint.path
     */
    String FLINK_CHECKPOINT_PATH = "flink.checkpoint.path";

    /**
     * 获取 FLINK_PRINT_LOG
     *
     * @return boolean
     */
    static boolean flinkPrintLog() {
        return Boolean.parseBoolean(getConfig(FLINK_PRINT_LOG, "false"));
    }

    /**
     * 获取 TOPIC_EVENT
     *
     * @return String
     */
    static String topicEvent() {
        return getConfig(TOPIC_EVENT);
    }

    /**
     * 获取 TOPIC_STAT
     *
     * @return String
     */
    static String topicStat() {
        return getConfig(TOPIC_STAT);
    }

    /**
     * 获取 HBASE_TABLE_STAT
     *
     * @return String
     */
    static String hbaseTableStat() {
        return getConfig(HBASE_TABLE_STAT);
    }

    /**
     * 获取 HBASE_BUFFER_FLUSH_MAX_IN_BYTES
     *
     * @return long
     */
    static long hbaseBufferFlushMaxSizeInBytes() {
        return Long.parseLong(getConfig(HBASE_BUFFER_FLUSH_MAX_IN_BYTES, "10240"));
    }

    /**
     * 获取 HBASE_BUFFER_FLUSH_MAX_MUTATIONS
     *
     * @return long
     */
    static long hbaseBufferFlushMaxMutations() {
        return Long.parseLong(getConfig(HBASE_BUFFER_FLUSH_MAX_MUTATIONS, "10"));
    }

    /**
     * 获取 HBASE_BUFFER_FLUSH_INTERVAL_MILLIS
     *
     * @return long
     */
    static long hbaseBufferFlushIntervalMillis() {
        return Long.parseLong(getConfig(HBASE_BUFFER_FLUSH_INTERVAL_MILLIS, "1000"));
    }

    /**
     * 获取 FLINK_ENV_PARALLELISM
     *
     * @return int
     */
    static int flinkEnvParallelism() {
        return Integer.parseInt(getConfig(FLINK_ENV_PARALLELISM, "32"));
    }

    /**
     * 获取 FLINK_SINK_HBASE_PARALLELISM
     *
     * @return int
     */
    static int flinkSinkHbaseParallelism() {
        return Integer.parseInt(getConfig(FLINK_SINK_HBASE_PARALLELISM, "32"));
    }

    /**
     * 获取 FLINK_SOURCE_KAFKA_PARALLELISM
     *
     * @return int
     */
    static int flinkSourceKafkaParallelism() {
        return Integer.parseInt(getConfig(FLINK_SOURCE_KAFKA_PARALLELISM, "32"));
    }

    /**
     * 获取 FLINK_AGGREGATE_PARALLELISM
     *
     * @return int
     */
    static int flinkAggregateParallelism() {
        return Integer.parseInt(getConfig(FLINK_AGGREGATE_PARALLELISM, "32"));
    }

    /**
     * 获取 FLINK_WATERMARK_INTERVAL
     *
     * @return long
     */
    static long flinkWatermarkInterval() {
        return Long.parseLong(getConfig(FLINK_WATERMARK_INTERVAL, "1000"));
    }

    /**
     * 获取 FLINK_CHECKPOINT_PATH
     *
     * @return String
     */
    static String flinkCheckpointPath() {
        return getConfig(FLINK_CHECKPOINT_PATH);
    }

    /**
     * 从config中获取配置
     *
     * @param key key
     * @return value
     */
    static String getConfig(String key) {
        String value = PROPS.getProperty(key, null);
        Validate.notBlank(value, "Apollo配置不存在或值为空：" + key);
        return value;
    }

    /**
     * 从config中获取配置 可指定默认值
     *
     * @param key          key
     * @param defaultValue 默认值
     * @return value
     */
    static String getConfig(String key, String defaultValue) {
        String value = PROPS.getProperty(key, null);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    /**
     * 获取 CheckpointConfig
     *
     * @return Configuration
     */
    static org.apache.flink.configuration.Configuration getCheckpointConfig() {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.valueOf(
                getConfig(ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key(), "EXACTLY_ONCE")));
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(
                Long.parseLong(getConfig(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key(), "1000"))));
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(
                Long.parseLong(getConfig(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key(), "60000"))));
        configuration.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS,
                Integer.parseInt(getConfig(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.key(), "1")));
        configuration.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(
                Long.parseLong(getConfig(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS.key(), "1000"))));
        configuration.set(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER,
                Integer.parseInt(getConfig(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER.key(), "0")));
        configuration.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
                CheckpointConfig.ExternalizedCheckpointCleanup.valueOf(
                        getConfig(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT.key(), "RETAIN_ON_CANCELLATION")));
        return configuration;
    }

    /**
     * 获取 RestartStrategyConfiguration
     *
     * @return Configuration
     */
    static RestartStrategies.RestartStrategyConfiguration getRestartStrategyConfiguration() {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY,
                getConfig(RestartStrategyOptions.RESTART_STRATEGY.key(), "failure-rate"));
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
                Integer.parseInt(getConfig(
                        RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL.key(), "3")));
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                Duration.ofMillis(Long.parseLong(getConfig(
                        RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL.key(), "300000"))));
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY,
                Duration.ofMillis(Long.parseLong(getConfig(
                        RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY.key(), "10000"))));
        return RestartStrategies.fromConfiguration(configuration)
                .orElse(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(10)));
    }

    /**
     * 获取kafka consumer相关配置
     *
     * @return Properties
     */
    static Properties getKafkaConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getConfig(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, null));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                getConfig(ConsumerConfig.GROUP_ID_CONFIG, null));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                getConfig(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                getConfig(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000"));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                getConfig(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2"));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                getConfig(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                getConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                getConfig(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                getConfig(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000"));
        return props;
    }

    /**
     * 获取kafka producer相关配置
     *
     * @return Properties
     */
    static Properties getKafkaProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG,
                getConfig(ProducerConfig.ACKS_CONFIG, "all"));
        props.put(ProducerConfig.RETRIES_CONFIG,
                getConfig(ProducerConfig.RETRIES_CONFIG, "0"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                getConfig(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                getConfig(ProducerConfig.BATCH_SIZE_CONFIG, "100"));
        props.put(ProducerConfig.LINGER_MS_CONFIG,
                getConfig(ProducerConfig.LINGER_MS_CONFIG, "0"));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                getConfig(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                getConfig(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getConfig(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ""));
        return props;
    }

    /**
     * 获取HBase Configuration
     *
     * @return Configuration
     */
    static org.apache.hadoop.conf.Configuration getHbaseConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, getConfig(HConstants.ZOOKEEPER_QUORUM));
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, getConfig(HConstants.ZOOKEEPER_CLIENT_PORT, "2181"));
        return configuration;
    }

}
