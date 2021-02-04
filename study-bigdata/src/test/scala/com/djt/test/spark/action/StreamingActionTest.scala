package com.djt.test.spark.action

import com.djt.spark.action.impl.{TradeStatStreamingAction, WordCountStreamingAction}
import com.djt.utils.ParamConstant
import org.junit.Test

import java.util.Properties

/**
 * 实时流测试类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 10:25
 */
class StreamingActionTest extends AbsActionTest {

    override protected def setConfig(config: Properties): Unit = {
        config.setProperty(ParamConstant.SPARK_LOG_LEVEL, "ERROR")
        config.setProperty(ParamConstant.SPARK_STREAMING_DURATION_SECONDS, "1")
    }

    @Test
    def testTradeStatStreamingAction(): Unit = {
        config.setProperty(ParamConstant.SPARK_SOCKET_STREAM_HOST, "172.20.20.183")
        config.setProperty(ParamConstant.SPARK_SOCKET_STREAM_PORT, "6666")
        new TradeStatStreamingAction(config).action()
    }

    @Test
    def testWordCountStreamingAction(): Unit = {
        config.setProperty(ParamConstant.SPARK_SOCKET_STREAM_HOST, "172.20.20.183")
        config.setProperty(ParamConstant.SPARK_SOCKET_STREAM_PORT, "7777")
        new WordCountStreamingAction(config).action()
    }

}
