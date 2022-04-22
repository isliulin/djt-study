package com.djt.test.utils;

import org.junit.Test;

import java.util.BitSet;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-04-19
 */
public class BitMapTest {

    @Test
    public void test1() {
        int size = 1024;
        BitSet bitSet = new BitSet(size);
        for (int i = 0; i < size; i++) {
            if (i % 2 == 0) {
                bitSet.set(i);
            }
        }
        System.out.println(bitSet.size());
    }
}
