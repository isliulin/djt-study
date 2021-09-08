package com.djt.test.utils;

import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-09-04
 */
public class MathTest {

    /**
     * 判断数A是否是数B的倍数 且允许存在误差
     *
     * @param a 数A
     * @param b 数B
     * @param e 误差
     * @return boolean
     */
    public static boolean isMultiple(long a, long b, long e) {
        if (a < b) {
            return a > (b - e);
        }
        long m = (a / b) * b;
        return (a >= m && a <= (m + e)) || (a >= (m + b - e) && a <= (m + b));
    }

    @Test
    public void test1() {
        for (int i = 0; i < 1001; i++) {
            if (isMultiple(i, 100, 0)) {
                System.out.println(i);
            }
        }
    }
}
