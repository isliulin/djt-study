package com.djt.tools;

import cn.hutool.setting.dialect.Props;

import java.nio.charset.StandardCharsets;

/**
 * 基础工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public abstract class AbsTools {

    /**
     * 加载配置
     */
    protected Props props = Props.getProp("config.properties", StandardCharsets.UTF_8);

    abstract public void execute(String[] args) throws Exception;

}
