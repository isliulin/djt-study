package com.djt.test.utils;

import com.djt.utils.RandomUtils;
import org.junit.Test;

/**
 * 随机数工具类 测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-27
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

    @Test
    public void testRandomName() {
        for (int i = 0; i < 100; i++) {
            int sexFlag = i % 2 == 0 ? 1 : 2;
            String sex = sexFlag == 1 ? "男" : "女";
            System.out.println(sex + ":" + RandomUtils.getRandomName((byte) sexFlag));
        }
    }

}
