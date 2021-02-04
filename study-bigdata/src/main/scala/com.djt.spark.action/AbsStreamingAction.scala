package com.djt.spark.action

import com.djt.utils.ParamConstant
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

/**
 * Spark Streaming任务基础类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-04 9:45
 */
abstract class AbsStreamingAction(config: Properties) extends AbsSparkAction(config) {

    private var streamingContext: StreamingContext = _

    /**
     * 任务入口
     */
    override def action(): Unit = {
        LOG.info("任务开始...")
        val start = System.currentTimeMillis()

        LOG.info("任务结束...共耗时：{} 秒", (System.currentTimeMillis() - start) / 1000)
    }

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
}
