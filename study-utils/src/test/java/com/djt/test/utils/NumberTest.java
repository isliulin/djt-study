package com.djt.test.utils;

import org.junit.Test;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-04
 */
public class NumberTest {


    @Test
    public void testLongAdder() {
        LongAdder count = new LongAdder();
        for (int i = 0; i < 10; i++) {
            count.increment();
        }
        System.out.println(count.longValue());
    }

}
