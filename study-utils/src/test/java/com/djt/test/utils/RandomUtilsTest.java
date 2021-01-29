package com.djt.test.utils;

import org.junit.Test;

/**
 * 随机数工具类 测试类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-27 20:02
 */
public class RandomUtilsTest {


    @Test
    public void testRandomNumber() {
        System.out.println(RandomUtils.getRandomNumber(0, 10));
        System.out.println(RandomUtils.getRandomNumber(0L, 10L));
    }

    @Test
    public void testRandomDate() {
        String start = "2020-01-01 00:00:00";
        String end = "2020-12-31 23:59:59";
        System.out.println(RandomUtils.getRandomDate(start, end));
        System.out.println(RandomUtils.getRandomDate(start, end, RandomUtils.YMD));
    }

}
