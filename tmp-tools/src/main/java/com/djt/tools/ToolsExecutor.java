package com.djt.tools;

import com.djt.tools.impl.HdfsTools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 程序主类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsExecutor {

    private static final Logger log = LogManager.getLogger(ToolsExecutor.class);

    public static void main(String[] args) {
        AbsTools tools = new HdfsTools();
        tools.execute(null);
    }
}
