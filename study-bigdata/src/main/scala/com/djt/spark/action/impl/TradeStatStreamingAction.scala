package com.djt.spark.action.impl

import com.djt.spark.action.AbsStreamingAction
import com.djt.utils.{DataParseUtils, ParamConstant}
import org.apache.spark.streaming.{StateSpec, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 模拟实时交易统计
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
class TradeStatStreamingAction(config: Properties) extends AbsStreamingAction(config) {

    /**
     * 监听端口 实时处理
     * 累加 交易笔数 与 交易金额
     *
     * @param streamingContext ss
     */
    override def executeAction(streamingContext: StreamingContext): Unit = {
        val sc = streamingContext.sparkContext
        sc.setLogLevel(config.getProperty(ParamConstant.SPARK_LOG_LEVEL, "ERROR"))
        streamingContext.checkpoint("/user/spark/chekpoint/test-djt-stream2")

        //用来初始化的rdd
        val initRdd = sc.parallelize(List(("cnt", 1L), ("amt", 1L)))

        val dStream = getSocketTextStream(streamingContext).flatMap(str => {
            val tuple2List = new ListBuffer[(String, Long)]
            val jsonObject = DataParseUtils.parseJsonObject(str)
            val cntKey = "cnt" //交易笔数
            val cntValue = DataParseUtils.getNumFromJson(jsonObject, cntKey)
            tuple2List.append((cntKey, cntValue))
            val amtKey = "amt" //交易金额
            val amtValue = DataParseUtils.getNumFromJson(jsonObject, amtKey)
            tuple2List.append((amtKey, amtValue))
            tuple2List
        })

        val stateSpec = StateSpec.function(mappingAddFunction _).initialState(initRdd)
        dStream.mapWithState(stateSpec).print()
        //dStream.updateStateByKey(updateAddFunction).print()

        streamingContext.start()
        streamingContext.awaitTermination()
    }

}
