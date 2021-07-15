package com.djt.flink.job;

import cn.hutool.setting.Setting;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
public class FlinkJobTest {

    private final Setting setting = new Setting("config.properties");

    @Before
    public void before() {
        //setting.put(ConfigConstants.Socket.FLINK_SOCKET_STREAM_HOST, "172.20.20.183");
        //setting.put(ConfigConstants.Socket.FLINK_SOCKET_STREAM_PORT, "9191");
        //setting.put(ConfigConstants.Socket.FLINK_SOCKET_STREAM_DELIMITER, "\n");
    }

    @Test
    public void testSocketStreamFlinkJob() {
        AbsFlinkJob job = new SocketStreamFlinkJob("SocketStreamFlinkJob", setting);
        job.run();
    }

    @Test
    public void testKafkaStreamFlinkJob() {
        AbsFlinkJob job = new KafkaStreamFlinkJob("KafkaStreamFlinkJob", setting);
        job.run();
    }

}
