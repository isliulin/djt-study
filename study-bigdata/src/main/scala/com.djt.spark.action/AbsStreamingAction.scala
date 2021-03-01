package com.djt.spark.action

import com.djt.utils.ParamConstant
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, State, StreamingContext}

import java.util.Properties

/**
 * Spark Streaming任务基础类
 *
 * @author 　djt317@qq.com
 * @since  　2021-02-04
 */
abstract class AbsStreamingAction(config: Properties) extends AbsSparkAction(config) {

    @transient
    private var streamingContext: StreamingContext = _

    /**
     * 任务入口
     */
    override def action(): Unit = {
        LOG.info("任务开始...")
        val start = System.currentTimeMillis()
        try {
            getStreamingContext
            executeAction(streamingContext)
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
                    sc.setLogLevel(config.getProperty(ParamConstant.SPARK_LOG_LEVEL, "ERROR"))
                    val batchDuration = Seconds(config.getProperty(ParamConstant.SPARK_STREAMING_DURATION_SECONDS, "1").toLong)
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
        val host = config.getProperty(ParamConstant.SPARK_SOCKET_STREAM_HOST, "")
        val port = config.getProperty(ParamConstant.SPARK_SOCKET_STREAM_PORT, "")
        if (StringUtils.isAnyBlank(host, port)) {
            throw new IllegalArgumentException(ParamConstant.SPARK_SOCKET_STREAM_HOST + " 与 " +
                    ParamConstant.SPARK_SOCKET_STREAM_PORT + " 均不能为空!")
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

}
