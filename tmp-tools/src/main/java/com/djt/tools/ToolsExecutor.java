package com.djt.tools;

/**
 * 程序主类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsExecutor {

    public static void main(String[] args) {
        try {
            Class<?> toolClass = Class.forName(AbsTools.PROPS.getStr("execute.class"));
            AbsTools tools = (AbsTools) toolClass.newInstance();
            tools.execute(null);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
