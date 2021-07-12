package com.djt.spark.action

import com.djt.utils.ConfigConstant
import org.apache.commons.lang3.{StringUtils, Validate}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StreamingContext}

import java.util.Properties

/**
 * Spark Streaming任务基础类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-04
 */
abstract class AbsStreamingAction(config: Properties) extends AbsSparkAction(config) {

    @transient
    private var streamingContext: StreamingContext = _

    override protected def setSparkConf(sparkConf: SparkConf): Unit = {
        super.setSparkConf(sparkConf)
        sparkConf.set("spark.streaming.backpressure.enabled", config.getProperty(ConfigConstant.Spark.SPARK_STREAMING_BACKPRESSURE, "true"))
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", config.getProperty(ConfigConstant.Spark.SPARK_STREAMING_KAFKA_MAX_RATE, "100"))
        sparkConf.set("spark.streaming.kafka.consumer.poll.ms", config.getProperty(ConfigConstant.Spark.SPARK_STREAMING_KAFKA_CONSUMER_POLL, "5000"))
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", config.getProperty(ConfigConstant.Spark.SPARK_STREAMING_STOP_GRACEFULLY, "true"))
    }

    /**
     * 任务入口
     */
    override def action(): Unit = {
        LOG.info("任务开始...")
        val start = System.currentTimeMillis()
        try {
            getStreamingContext
            executeAction(streamingContext)
            streamingContext.start()
            streamingContext.awaitTermination()
        } catch {
            case e: Exception =>
                LOG.error("系统异常！", e)
        } finally {
            streamingContext.stop()
            LOG.info("任务结束...共耗时：{} 秒", (System.currentTimeMillis() - start) / 1000)
        }
    }

    /**
     * 任务执行实体 由子类实现
     *
     * @param streamingContext sc
     */
    def executeAction(streamingContext: StreamingContext): Unit

    /**
     * 忽略Spark任务
     *
     * @param sparkSession ss
     */
    def executeAction(sparkSession: SparkSession): Unit = {}

    /**
     * 获取 StreamingContext
     *
     * @return ssc
     */
    private def getStreamingContext: StreamingContext = {
        if (null == streamingContext) {
            this.synchronized {
                if (null == streamingContext) {
                    val sc = getSparkSession.sparkContext
                    sc.setLogLevel(config.getProperty(ConfigConstant.Spark.SPARK_LOG_LEVEL, "ERROR"))
                    val batchDuration = Seconds(config.getProperty(ConfigConstant.Spark.SPARK_STREAMING_DURATION_SECONDS, "5").toLong)
                    streamingContext = new StreamingContext(sc, batchDuration)
                }
            }
        }
        streamingContext
    }

    /**
     * 获取Socket输入流
     * config中需要设置主机与端口号
     *
     * @param streamingContext sc
     * @return dStream
     */
    protected def getSocketTextStream(streamingContext: StreamingContext): ReceiverInputDStream[String] = {
        val host = config.getProperty(ConfigConstant.Spark.SPARK_SOCKET_STREAM_HOST, "")
        val port = config.getProperty(ConfigConstant.Spark.SPARK_SOCKET_STREAM_PORT, "")
        if (StringUtils.isAnyBlank(host, port)) {
            throw new IllegalArgumentException(ConfigConstant.Spark.SPARK_SOCKET_STREAM_HOST + " 与 " +
                    ConfigConstant.Spark.SPARK_SOCKET_STREAM_PORT + " 均不能为空!")
        }
        streamingContext.socketTextStream(host, port.toInt)
    }

    /**
     * mapWithState 专用函数
     * 累加最新值 = 历史值 + 当前值
     *
     * @param key   key
     * @param value value
     * @param state 历史状态
     * @return
     */
    def mappingAddFunction(key: String, value: Option[Long], state: State[Long]): (String, Long) = {
        if (state.isTimingOut()) {
            LOG.warn("{} 已过期!", key)
            return (key, 0L)
        }
        val sumStateValue = state.getOption().getOrElse(0L) + value.getOrElse(0L)
        state.update(sumStateValue)
        (key, sumStateValue)
    }

    /**
     * updateStateByKey 专用函数
     * 累加最新值 = 历史值 + 当前值
     *
     * @param values 相同key对应的所有value值
     * @param state  历史状态
     * @return 最新结果
     */
    def updateAddFunction(values: Seq[Long], state: Option[Long]): Option[Long] = {
        val sumValue = state.getOrElse(0L) + values.sum
        Some(sumValue)
    }

    /**
     * 获取kafka配置
     *
     * @return
     */
    def getKafkaParams: Map[String, Object] = {
        Map[String, Object](
            "bootstrap.servers" -> config.getProperty(ConfigConstant.Kafka.KAFKA_BOOTSTRAP_SERVERS),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> config.getProperty(ConfigConstant.Kafka.KAFKA_GROUP_ID, "SPARK_ETL_DJT"),
            "auto.offset.reset" -> config.getProperty(ConfigConstant.Kafka.KAFKA_AUTO_OFFSET_RESET, "latest"),
            "enable.auto.commit" -> config.getProperty(ConfigConstant.Kafka.KAFKA_ENABLE_AUTO_COMMIT, "false"),
            "session.timeout.ms" -> config.getProperty(ConfigConstant.Kafka.KAFKA_SESSION_TIMEOUT_MS, "30000"),
            "heartbeat.interval.ms" -> config.getProperty(ConfigConstant.Kafka.KAFKA_HEARTBEAT_INTERVAL_MS, "5000"),
            "max.poll.records" -> config.getProperty(ConfigConstant.Kafka.KAFKA_MAX_POLL_RECORDS, "10"),
            "max.poll.interval.ms" -> config.getProperty(ConfigConstant.Kafka.KAFKA_MAX_POLL_INTERVAL_MS, "300000"))
    }

    /**
     * 创建输入流
     *
     * @return
     */
    def createDirectStream(): InputDStream[ConsumerRecord[String, String]] = {
        val topics = config.getProperty(ConfigConstant.Kafka.KAFKA_CONSUMER_TOPICS)
        Validate.notBlank(topics, s"${ConfigConstant.Kafka.KAFKA_CONSUMER_TOPICS} can not be null!")
        val topicList = topics.split(",").map(_.trim)
        KafkaUtils.createDirectStream[String, String](
            getStreamingContext,
            PreferConsistent,
            Subscribe[String, String](topicList, getKafkaParams))
    }

}
