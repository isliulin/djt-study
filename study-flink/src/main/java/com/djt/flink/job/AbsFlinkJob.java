package com.djt.flink.job;

import cn.hutool.setting.Setting;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * 抽象Flink基础类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
@Log4j2
abstract public class AbsFlinkJob {

    /**
     * 任务名
     */
    protected final String jobName;

    /**
     * 任务配置
     */
    protected Setting setting;

    /**
     * Flink运行环境
     */
    private final StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();


    public AbsFlinkJob(String jobName, Setting setting) {
        this.jobName = jobName;
        this.setting = setting;

    }

    /**
     * 任务执行入口
     */
    public void run() {
        try {
            executeBefore();
            doRun(exeEnv);
        } catch (Exception e) {
            log.error("任务运行异常: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * 任务执行前的动作
     */
    protected void executeBefore() throws Exception {
    }

    /**
     * 任务主体
     */
    abstract protected void doRun(StreamExecutionEnvironment exeEnv) throws Exception;

    /**
     * 获取kafka相关配置
     *
     * @return Properties
     */
    public Properties getKafkaProps() {
        return setting.getProperties("kafka");
    }
}
