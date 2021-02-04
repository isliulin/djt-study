package com.djt.test.spark.action

import com.djt.spark.action.impl.TradeStatStreamingAction
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
    def testStreamingAction(): Unit = {
        new TradeStatStreamingAction(config).action()
    }

}
