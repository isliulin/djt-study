package com.djt.spark.action.impl

import com.djt.spark.action.AbsSparkAction
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 模拟实时单词计数
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 9:47
 */
class WordCountStreamingAction(config: Properties) extends AbsSparkAction(config) {

    /**
     * 监听端口 实时处理
     *
     * @param sparkSession ss
     */
    override def executeAction(sparkSession: SparkSession): Unit = {
        val sc = sparkSession.sparkContext
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(1))
        ssc.checkpoint("/user/spark/chekpoint/test-djt-stream2")

        //用来初始化的rdd
        val initRdd = sc.parallelize(List(("cnt", 1L), ("amt", 1L)))

        val dStream = ssc.socketTextStream("172.20.20.183", 6666).flatMap(str => {
            val tuple2List = new ListBuffer[(String, Long)]
            val jsonObject = TradeStatStreamingAction.parseJsonObject(str)
            val cntKey = "cnt"
            val cntValue = TradeStatStreamingAction.getNumFromJson(jsonObject, cntKey)
            tuple2List.append((cntKey, cntValue))
            val amtKey = "amt"
            val amtValue = TradeStatStreamingAction.getNumFromJson(jsonObject, amtKey)
            tuple2List.append((amtKey, amtValue))
            tuple2List
        })

        val stateSpec = StateSpec.function(mappingFunction _).initialState(initRdd)
        dStream.mapWithState(stateSpec).print()
        //dStream.updateStateByKey(mappingFunction2).print()

        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * mapWithState 专用函数
     *
     * @param key   key
     * @param value value
     * @param state 上次结果
     * @return
     */
    def mappingFunction(key: String, value: Option[Long], state: State[Long]): (String, Long) = {
        // 获取历史值
        val historyStateValue = state.getOption().getOrElse(0L)
        // 最新值 = 当前值 + 历史值
        val currentStateValue = historyStateValue + value.getOrElse(0L)
        // 更新状态值
        state.update(currentStateValue)
        // 返回结果
        (key, currentStateValue)
    }

    /**
     * updateStateByKey 专用函数
     *
     * @param values 相同key的所有value值
     * @param state  上次结果
     * @return 最新结果
     */
    def mappingFunction2(values: Seq[Long], state: Option[Long]): Option[Long] = {
        // 获取历史值
        val historySum = state.getOrElse(0L)
        // 计算当前值
        val currentSum = values.sum
        // 最新值 = 当前值 + 历史值
        val sum = currentSum + historySum
        // 返回结果
        Some(sum)
    }
}


