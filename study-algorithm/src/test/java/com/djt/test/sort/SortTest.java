package com.djt.test.sort;

import com.djt.algorithm.sort.IArraySort;
import com.djt.algorithm.sort.impl.*;
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
    public void testAllSort() {
        sortAndPrint(new BubbleSort());
        sortAndPrint(new SelectSort());
        sortAndPrint(new QuickSort());
        sortAndPrint(new InsertSort());
        sortAndPrint(new ShellSort());
        sortAndPrint(new MergeSort());
    }

    @Test
    public void testBubbleSort() {
        sortAndPrint(new BubbleSort());
    }

    @Test
    public void testSelectSort() {
        sortAndPrint(new SelectSort());
    }

    @Test
    public void testQuickSort() {
        sortAndPrint(new QuickSort());
    }

    @Test
    public void testInsertSort() {
        sortAndPrint(new InsertSort());
    }

    @Test
    public void testShellSort() {
        sortAndPrint(new ShellSort());
    }

    @Test
    public void testMergeSort() {
        sortAndPrint(new MergeSort());
    }

    private void sortAndPrint(IArraySort sort) {
        System.out.println(sort.getName());
        System.out.println("排序前 " + Arrays.toString(arr));
        System.out.println("排序后 " + Arrays.toString(sort.sort(arr)));
    }

}
