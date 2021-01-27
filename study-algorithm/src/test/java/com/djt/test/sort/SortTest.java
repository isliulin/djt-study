package com.djt.test.sort;

import com.djt.sort.IArraySort;
import com.djt.sort.impl.BubbleSort;
import com.djt.test.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * 排序测试类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-27 17:33
 */
public class SortTest {

    private final int[] arr = new int[10];

    @Before
    public void before() {
        for (int i = 0; i < 10; i++) {
            arr[i] = TestUtils.getRandomNumber(0, 10);
        }
    }

    @Test
    public void testBubbleSort() throws Exception {
        IArraySort arraySort = new BubbleSort();
        System.out.println(Arrays.toString(arr));
        System.out.println(Arrays.toString(arraySort.sort(arr)));
    }
}
