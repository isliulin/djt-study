package com.djt.test.utils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.djt.test.bean.Log4j2PrintStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
public class LogTest {

    @Test
    public void testLog1() {
        Log log = LogFactory.get();
        log.info("{} 666", 123);
    }

    @Test
    public void testLog2() {
        Logger log = LogManager.getLogger(this.getClass());
        log.info("666");
        System.out.println(MarkerManager.exists("sink"));
        Marker marker = MarkerManager.getMarker("sink");
        log.info(marker, "666");
    }

    @Test
    public void testLog3() {
        Log4j2PrintStream.redirectSystemPrint();
        System.out.println("666");
        System.out.println("666");
        System.out.println((Object) null);
        System.out.println((LogTest) null);
    }


}
