package com.djt.spark.action.impl

import com.djt.spark.action.AbsStreamingAction
import com.djt.utils.ParamConstant
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.{StateSpec, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 模拟实时统计单词频率
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 9:47
 */
class WordCountStreamingAction(config: Properties) extends AbsStreamingAction(config) {

    /**
     * 监听端口 实时统计单词频率
     *
     * @param streamingContext sc
     */
    override def executeAction(streamingContext: StreamingContext): Unit = {
        streamingContext.sparkContext.setLogLevel(config.getProperty(ParamConstant.SPARK_LOG_LEVEL, "ERROR"))
        streamingContext.checkpoint("/user/spark/chekpoint/test-djt-stream2")

        val dStream = getSocketTextStream(streamingContext).flatMap(str => {
            val tuple2List = new ListBuffer[(String, Long)]
            val wordArr = StringUtils.split(str, " ")
            if (wordArr != null) {
                wordArr.foreach(word => {
                    tuple2List.append((word, 1L))
                })
            }
            tuple2List
        }) //.reduceByKey(_ + _)

        //使用 mapWithState 需要提前 reduceByKey 进行预汇总 如果不预先汇总 则结果必然错误 最后一步汇总结果更离谱！
        val stateSpec = StateSpec.function(mappingAddFunction _)
        dStream.mapWithState(stateSpec).print()
        //使用 updateStateByKey 则无上述问题
        //dStream.updateStateByKey(updateAddFunction).print()

        streamingContext.start()
        streamingContext.awaitTermination()
    }

}


