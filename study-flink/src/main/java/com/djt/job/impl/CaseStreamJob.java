package com.djt.job.impl;

import com.djt.job.BaseStreamJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink实时任务示例
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
public class CaseStreamJob extends BaseStreamJob {

    public CaseStreamJob(String jobName) {
        super(jobName);
    }

    public CaseStreamJob(String jobName, StreamExecutionEnvironment streamEnv) {
        super(jobName, streamEnv);
    }

    @Override
    protected void doRun(StreamExecutionEnvironment streamEnv) throws Exception {

    }
}
