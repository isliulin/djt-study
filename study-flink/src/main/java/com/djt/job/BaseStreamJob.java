package com.djt.job;

import com.djt.utils.ConfigConstants;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink流任务抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-20
 */
@Log4j2
abstract public class BaseStreamJob {

    /**
     * 任务名
     */
    protected final String jobName;

    /**
     * Flink运行环境
     */
    private final StreamExecutionEnvironment streamEnv;

    public BaseStreamJob(String jobName) {
        this(jobName, StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public BaseStreamJob(String jobName, StreamExecutionEnvironment streamEnv) {
        this.jobName = jobName;
        this.streamEnv = streamEnv;
        streamEnv.setParallelism(ConfigConstants.flinkEnvParallelism());
        streamEnv.getConfig().setAutoWatermarkInterval(ConfigConstants.flinkWatermarkInterval());
        streamEnv.setStateBackend(new FsStateBackend(ConfigConstants.flinkCheckpointPath()));
        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.configure(ConfigConstants.getCheckpointConfig());
    }

    /**
     * 任务执行入口
     */
    public void run() {
        try {
            doInit();
            doRun(streamEnv);
        } catch (Exception e) {
            log.error("任务执行失败: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化
     */
    protected void doInit() {}

    /**
     * 任务主体 由子类实现
     *
     * @param streamEnv env
     * @throws Exception e
     */
    abstract protected void doRun(StreamExecutionEnvironment streamEnv) throws Exception;

}
