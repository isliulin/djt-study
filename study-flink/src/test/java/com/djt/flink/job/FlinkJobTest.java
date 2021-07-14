package com.djt.flink.job;

import cn.hutool.setting.dialect.Props;
import com.djt.flink.utils.ConfigConstants;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
public class FlinkJobTest {

    Props props = new Props();

    @Before
    public void before() {
        props.setProperty(ConfigConstants.Socket.FLINK_SOCKET_STREAM_HOST, "172.20.20.183");
        props.setProperty(ConfigConstants.Socket.FLINK_SOCKET_STREAM_PORT, "9191");
        props.setProperty(ConfigConstants.Socket.FLINK_SOCKET_STREAM_DELIMITER, "\n");
    }

    @Test
    public void testSocketStreamFlinkJob() {
        AbsFlinkJob job = new SocketStreamFlinkJob("SocketStreamFlinkJob", props);
        job.run();
    }

}
