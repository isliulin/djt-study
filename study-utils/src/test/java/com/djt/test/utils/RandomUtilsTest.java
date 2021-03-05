package com.djt.test.utils;

import com.djt.utils.RandomUtils;
import org.junit.Test;

import java.util.Random;

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
        System.out.println(Long.MAX_VALUE);
        System.out.println(RandomUtils.getRandomNumber(Long.MAX_VALUE - 2, Long.MAX_VALUE));
    }

    @Test
    public void testRandomNumber2() {
        int min = 1;
        int max = 3;
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt(max + 1) % (max - min + 1) + min);
        }
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
