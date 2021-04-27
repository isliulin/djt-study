package com.djt.utils

import org.apache.hadoop.hbase.HConstants

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
object ParamConstant {

    /**
     * spark master
     */
    val SPARK_MASTER = "spark.master"

    /**
     * spark app name
     */
    val SPARK_APP_NAME = "spark.app.name"

    /**
     * spark log level
     */
    val SPARK_LOG_LEVEL = "spark.log.level"

    /**
     * spark streaming batchDuration
     */
    val SPARK_STREAMING_DURATION_SECONDS = "spark.streaming.duration.seconds"

    /**
     * spark socketTextStream host
     */
    val SPARK_SOCKET_STREAM_HOST = "spark.socket.stream.host"

    /**
     * spark socketTextStream port
     */
    val SPARK_SOCKET_STREAM_PORT = "spark.socket.stream.port"

    /**
     * spark
     */
    val MAX_TOSTRING_FIELDS = "spark.debug.maxToStringFields"

    /**
     * spark streaming 背压机制
     */
    val SPARK_STREAMING_BACKPRESSURE = "spark.streaming.backpressure.enabled"

    /**
     * spark streaming kafka每个分区最大读取条数
     */
    val SPARK_STREAMING_KAFKA_MAX_RATE = "spark.streaming.kafka.maxRatePerPartition"

    /**
     * spark streaming kafka consumer poll时间
     */
    val SPARK_STREAMING_KAFKA_CONSUMER_POLL = "spark.streaming.kafka.consumer.poll.ms"

    /**
     * spark streaming 是否优雅停止
     */
    val SPARK_STREAMING_STOP_GRACEFULLY = "spark.streaming.stopGracefullyOnShutdown"

    /**
     * hbase zk节点
     */
    val HBASE_ZK_QUORUM: String = HConstants.ZOOKEEPER_QUORUM

    /**
     * hbase zk端口
     */
    val HBASE_ZK_PORT: String = HConstants.ZOOKEEPER_CLIENT_PORT

    /**
     * es 集群节点
     */
    val ES_NODES = "es.nodes"

    /**
     * es 集群端口
     */
    val ES_PORT = "es.port"

    /**
     * es 地址 IP+端口
     */
    val ES_HOST = "es.host"

    /**
     * es 是否自动创建索引
     */
    val ES_INDEX_AUTO_CREATE = "es.index.auto.create"

    /**
     * es 更新冲突重试次数
     */
    val ES_UPDATE_RETRY_ON_CONFLICT = "es.update.retry.on.conflict"

    /**
     * es日期字段处理
     */
    val ES_MAPPING_DATE_RICH = "es.mapping.date.rich"

    /**
     * kudu地址
     */
    val KUDU_MASTER = "kudu.master"

    /**
     * phoenix zk地址
     */
    val PHOENIX_ZK_URL = "phoenix.zk.url"

    /**
     * Kafka 地址
     */
    val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"

    /**
     * Kafka 消费主题列表
     */
    val KAFKA_CONSUMER_TOPICS = "kafka.consumer.topics"

    /**
     * Kafka 消费者组
     */
    val KAFKA_GROUP_ID = "kafka.group.id"

    /**
     * Kafka 初始化消费起点
     */
    val KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset"

    /**
     * Kafka 自动提交
     */
    val KAFKA_ENABLE_AUTO_COMMIT = "kafka.enable.auto.commit"

    /**
     * Kafka 会话超时时间
     */
    val KAFKA_SESSION_TIMEOUT_MS = "kafka.session.timeout.ms"

    /**
     * Kafka 心跳发送间隔
     */
    val KAFKA_HEARTBEAT_INTERVAL_MS = "kafka.heartbeat.interval.ms"

    /**
     * Kafka 单次拉取最大消息数
     */
    val KAFKA_MAX_POLL_RECORDS = "kafka.max.poll.records"

    /**
     * Kafka 最大数据处理时间(此参数在0.10.1及之后的版本才有)
     */
    val KAFKA_MAX_POLL_INTERVAL_MS = "kafka.max.poll.interval.ms"


}
