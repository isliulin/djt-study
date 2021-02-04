package com.djt.test.utils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 10:00
 */
public class LogTest {

    private static final Log log = LogFactory.get();

    @Test
    public void testLog() {
        log.info("{} 666", 123);
    }
}
