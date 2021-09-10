package com.djt;

import com.djt.job.impl.CaseStreamJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * 启动类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
public class ApplicationTest {

    @Test
    public void main() {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        new CaseStreamJob("djt-study-flink", streamEnv).run();
    }

}
