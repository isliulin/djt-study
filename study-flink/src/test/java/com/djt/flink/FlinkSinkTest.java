package com.djt.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.event.MyEvent;
import com.djt.sink.HBaseData;
import com.djt.sink.HBaseDataConverter;
import com.djt.utils.ConfigConstants;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Sink 测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-18
 */
public class FlinkSinkTest extends FlinkBaseTest {

    @Test
    public void testHBaseSink() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSource();
        Configuration configuration = ConfigConstants.getHbaseConfiguration();
        SingleOutputStreamOperator<HBaseData> hbaseDataStream = kafkaSource.map(event -> {
            JSONObject data = JSON.parseObject(JSON.toJSONString(event));
            return new HBaseData(event.getId(), data);
        });

        hbaseDataStream.addSink(new HBaseSinkFunction<>("TEST:T_TEST_DJT", configuration,
                new HBaseDataConverter(), 1024 * 10L, 10L, 1000L));

        hbaseDataStream.print("写入HBase======");

        streamEnv.execute("testHBaseSink");
    }

}
