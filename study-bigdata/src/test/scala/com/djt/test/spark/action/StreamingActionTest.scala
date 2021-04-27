package com.djt.test.spark.action

import com.djt.spark.action.impl.{KafkaStreamingAction, TradeStatStreamingAction, WordCountStreamingAction}
import com.djt.utils.ParamConstant
import org.junit.Test

import java.util.Properties

/**
 * 实时流测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
class StreamingActionTest extends AbsActionTest {

    override protected def setConfig(config: Properties): Unit = {
        config.setProperty(ParamConstant.SPARK_LOG_LEVEL, "WARN")
        config.setProperty(ParamConstant.KAFKA_BOOTSTRAP_SERVERS, "172.20.7.36:9092,172.20.7.37:9092,172.20.7.38:9092")
        config.setProperty(ParamConstant.SPARK_STREAMING_DURATION_SECONDS, "5")
        config.setProperty(ParamConstant.KAFKA_GROUP_ID, "SPARK_ETL_DJT")
        config.setProperty(ParamConstant.KAFKA_AUTO_OFFSET_RESET, "latest")
        config.setProperty(ParamConstant.KAFKA_ENABLE_AUTO_COMMIT, "false")
        config.setProperty(ParamConstant.KAFKA_SESSION_TIMEOUT_MS, "30000")
        config.setProperty(ParamConstant.KAFKA_HEARTBEAT_INTERVAL_MS, "5000")
        config.setProperty(ParamConstant.KAFKA_MAX_POLL_INTERVAL_MS, "300000")
        config.setProperty(ParamConstant.KAFKA_MAX_POLL_RECORDS, "10")

        config.setProperty(ParamConstant.SPARK_STREAMING_BACKPRESSURE, "true")
        config.setProperty(ParamConstant.SPARK_STREAMING_KAFKA_MAX_RATE, "100")
        config.setProperty(ParamConstant.SPARK_STREAMING_KAFKA_CONSUMER_POLL, "5000")
        config.setProperty(ParamConstant.SPARK_STREAMING_STOP_GRACEFULLY, "true")
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

    @Test
    def testKafkaStreamingAction(): Unit = {
        config.setProperty(ParamConstant.KAFKA_CONSUMER_TOPICS, "TEST_DJT")
        config.setProperty(ParamConstant.SPARK_STREAMING_DURATION_SECONDS, "5")
        config.setProperty(ParamConstant.KAFKA_SESSION_TIMEOUT_MS, "30000")
        config.setProperty(ParamConstant.KAFKA_HEARTBEAT_INTERVAL_MS, "5000")
        config.setProperty(ParamConstant.KAFKA_MAX_POLL_INTERVAL_MS, "300000")
        config.setProperty(ParamConstant.KAFKA_MAX_POLL_RECORDS, "10")

        config.setProperty(ParamConstant.SPARK_STREAMING_BACKPRESSURE, "true")
        config.setProperty(ParamConstant.SPARK_STREAMING_KAFKA_MAX_RATE, "10")
        config.setProperty(ParamConstant.SPARK_STREAMING_KAFKA_CONSUMER_POLL, "3000")

        new KafkaStreamingAction(config).action()
    }

}
