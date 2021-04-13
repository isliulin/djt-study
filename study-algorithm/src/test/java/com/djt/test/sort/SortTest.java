package com.djt.test.sort;

import com.djt.algorithm.sort.IArraySort;
import com.djt.algorithm.sort.impl.BubbleSort;
import com.djt.algorithm.sort.impl.InsertSort;
import com.djt.algorithm.sort.impl.QuickSort;
import com.djt.algorithm.sort.impl.SelectSort;
import com.djt.utils.RandomUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * 排序测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-27
 */
public class SortTest {

    private final int ARRAY_SIZE = 10;
    private final int[] arr = new int[ARRAY_SIZE];

    @Before
    public void before() {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            arr[i] = RandomUtils.getRandomNumber(0, 10);
        }
    }

    @Test
    public void testBubbleSort() throws Exception {
        IArraySort arraySort = new BubbleSort();
        printArray("排序前", arr);
        printArray("排序后", arraySort.sort(arr));
    }

    @Test
    public void testSelectSort() throws Exception {
        IArraySort arraySort = new SelectSort();
        printArray("排序前", arr);
        printArray("排序后", arraySort.sort(arr));
    }

    @Test
    public void testQuickSort() throws Exception {
        IArraySort arraySort = new QuickSort();
        printArray("排序前", arr);
        printArray("排序后", arraySort.sort(arr));
    }

    @Test
    public void testInsertSort() throws Exception {
        IArraySort arraySort = new InsertSort();
        printArray("排序前", arr);
        printArray("排序后", arraySort.sort(arr));
    }

    public static void printArray(String msg, int[] arr) {
        System.out.println(msg + " " + Arrays.toString(arr));
    }


}
