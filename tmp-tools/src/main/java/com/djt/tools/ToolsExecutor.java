package com.djt.tools;

import com.djt.tools.impl.HdfsTools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsExecutor {

    private static final Logger log = LogManager.getLogger(ToolsExecutor.class);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            new HdfsTools().execute(null);
        } catch (Exception e) {
            log.error("程序运行出错：{}", e.getMessage());
        } finally {
            long stop = System.currentTimeMillis();
            log.info("运行完成...耗时：{} s", (stop - start) / 1000);
        }
    }
}
