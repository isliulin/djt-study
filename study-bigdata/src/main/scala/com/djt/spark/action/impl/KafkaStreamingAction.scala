package com.djt.spark.action.impl

import com.djt.spark.action.AbsStreamingAction
import com.djt.utils.DjtConstant
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

import java.time.{Instant, LocalDateTime}
import java.util.Properties

/**
 * Spark 读取 Kafka 实时任务
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-26
 */
class KafkaStreamingAction(config: Properties) extends AbsStreamingAction(config) {

    /**
     * 主流程
     *
     * @param streamingContext sc
     */
    override def executeAction(streamingContext: StreamingContext): Unit = {
        val directStream = createDirectStream()
        directStream.foreachRDD((rdd, batch) => {
            val startTime = System.currentTimeMillis()
            //转换批次号
            val batchTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(batch.milliseconds / 1000), DjtConstant.ZONE_ID).format(DjtConstant.YMDHMS_FORMAT)
            LOG.warn(s"=======批次：$batchTime 处理开始...=======")
            //获取offset
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            if (!rdd.isEmpty()) {
                offsetRanges.foreach(offsetRange => {
                    LOG.warn(offsetRange.toString())
                })
            }

            //处理逻辑开始==================================================================
            val dataCount = rdd.count()
            //处理逻辑结束==================================================================

            //手动提交offset
            directStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

            val stopTime = System.currentTimeMillis()
            LOG.warn(s"数据条数：$dataCount 处理耗时：${stopTime - startTime} ms")
            LOG.warn(s"=======批次：$batchTime 处理结束...=======")
        })
    }
}
