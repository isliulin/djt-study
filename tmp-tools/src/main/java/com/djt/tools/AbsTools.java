package com.djt.tools;

import cn.hutool.setting.dialect.Props;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

/**
 * 抽象基础工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public abstract class AbsTools {

    private static final Logger LOG = LogManager.getLogger(AbsTools.class);

    /**
     * 配置文件路径
     */
    public static final String CONFIG_PATH = "file:" + System.getProperty("user.dir") + "/config/config.properties";

    /**
     * 加载配置
     */
    public static final Props PROPS = Props.getProp(CONFIG_PATH, StandardCharsets.UTF_8);

    public void execute(String[] args) {
        LOG.info("程序开始运行...");
        long start = System.currentTimeMillis();
        try {
            doExecute(args);
        } catch (Exception e) {
            LOG.error("程序运行出错!", e);
        } finally {
            long stop = System.currentTimeMillis();
            LOG.info("运行完成...耗时：{} s", (stop - start) / 1000);
        }
    }

    abstract public void doExecute(String[] args) throws Exception;

}
