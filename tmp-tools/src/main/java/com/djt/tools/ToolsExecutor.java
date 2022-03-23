package com.djt.tools;

import cn.hutool.core.util.ArrayUtil;

import static com.djt.tools.AbsTools.PROPS;

/**
 * 程序主类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsExecutor {

    /**
     * 基础路径
     */
    public static final String PACKAGE_PATH = "com.djt.tools.impl.";

    public static void main(String[] args) {
        try {
            if (ArrayUtil.isNotEmpty(args)) {
                String classPath = PACKAGE_PATH + args[0];
                PROPS.setProperty("execute.class", classPath);
            }
            Class<?> toolClass = Class.forName(PROPS.getStr("execute.class"));
            AbsTools tools = (AbsTools) toolClass.newInstance();
            tools.execute(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
