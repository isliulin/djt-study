package com.djt.tools;

import cn.hutool.setting.dialect.Props;
import lombok.extern.log4j.Log4j2;

import java.nio.charset.StandardCharsets;

/**
 * 抽象基础工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
@Log4j2
public abstract class AbsTools {

    /**
     * 配置文件路径
     */
    public static final String CONFIG_PATH = "file:" + System.getProperty("user.dir") + "/config/config.properties";

    /**
     * 加载配置
     */
    public static final Props PROPS = Props.getProp(CONFIG_PATH, StandardCharsets.UTF_8);

    public void execute(String[] args) {
        log.info("程序开始运行,启动类为:{}", getClass().getName());
        long start = System.currentTimeMillis();
        try {
            doExecute(args);
        } catch (Exception e) {
            log.error("程序运行出错!", e);
        } finally {
            long stop = System.currentTimeMillis();
            log.info("运行完成...耗时：{} s", (stop - start) / 1000);
        }
    }

    /**
     * 任务主体 由子类实现
     *
     * @param args 参数类表
     * @throws Exception e
     */
    abstract public void doExecute(String[] args) throws Exception;

}
