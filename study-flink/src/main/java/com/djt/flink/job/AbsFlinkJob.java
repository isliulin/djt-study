package com.djt.flink.job;

import cn.hutool.setting.dialect.Props;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 抽象Flink基础类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
@Log4j2
abstract public class AbsFlinkJob {

    private final String jobName;
    protected StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    protected Props props;

    public AbsFlinkJob(String jobName, Props props) {
        this.jobName = jobName;
        this.props = props;
    }

    public void run() {
        try {
            executeBefore();
            executeAction();
            exeEnv.execute(jobName);
        } catch (Exception e) {
            log.error("任务运行异常: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    protected void executeBefore() {
    }

    abstract protected void executeAction();
}
