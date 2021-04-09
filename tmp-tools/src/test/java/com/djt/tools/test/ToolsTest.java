package com.djt.tools.test;

import com.djt.tools.impl.HdfsTools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsTest {

    private static final Logger log = LogManager.getLogger(ToolsTest.class);

    private static long start;

    @Before
    public void before() {
        start = System.currentTimeMillis();
    }

    @After
    public void after() {
        long stop = System.currentTimeMillis();
        log.info("运行完成...耗时：{} s", (stop - start) / 1000);
    }

    @Test
    public void testHdfsTools() throws Exception {
        new HdfsTools().execute(null);
    }
}
