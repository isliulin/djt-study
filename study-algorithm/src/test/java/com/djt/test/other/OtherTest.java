package com.djt.test.other;

import cn.hutool.core.util.StrUtil;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-04-02
 */
public class OtherTest {

    @Test
    public void testFibonacci() {
        for (int i = 1; i <= 100; i++) {
            long start = System.currentTimeMillis();
            long value = fib(i);
            long stop = System.currentTimeMillis();
            System.out.println(StrUtil.format("i={}, value={}, 耗时={}", i, value, (stop - start)));
        }
    }

    /**
     * 计算斐波那契数列
     *
     * @param index 位数
     * @return long
     */
    public long fib(long index) {
        if (index == 1 || index == 2) {
            return 1;
        }
        return fib(index - 1) + fib(index - 2);
    }

}
