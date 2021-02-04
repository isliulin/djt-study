package com.djt.utils

import org.apache.hadoop.hbase.HConstants

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 10:22
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

}
