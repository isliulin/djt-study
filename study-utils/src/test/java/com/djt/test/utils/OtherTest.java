package com.djt.test.utils;

import com.djt.utils.RandomUtils;
import org.junit.Test;

import java.time.LocalDate;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-07
 */
public class OtherTest {

    @Test
    public void test1() {
        int size = 30;
        LocalDate date = LocalDate.now();
        for (int i = 0; i < size; i++) {
            LocalDate dateTmp = date.minusDays(i);
            int num = Integer.parseInt(dateTmp.format(RandomUtils.YMD));
            System.out.println(num + "======" + num % size);
        }
    }
}
