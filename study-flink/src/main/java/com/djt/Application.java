package com.djt;

import com.djt.job.impl.CaseStreamJob;

/**
 * 启动类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
public class Application {

    public static void main(String[] args) {
        new CaseStreamJob("djt-study-flink").run();
    }
}
