package com.djt.test.heap;

import com.djt.algorithm.heap.Heap;
import com.djt.utils.RandomUtils;
import com.djt.utils.SortUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * 堆 测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-05-10
 */
public class HeapTest {

    private final int ARRAY_SIZE = 10;
    private final Integer[] arr = new Integer[ARRAY_SIZE];
    private Heap<Integer> heap = null;

    @Before
    public void before() {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            arr[i] = RandomUtils.getRandomNumber(0, 10);
        }
        heap = new Heap<>(arr, false);
    }

    @Test
    public void test1() {
        System.out.println(Arrays.toString(arr));
        System.out.println(Arrays.toString(heap.getHeapArr()));
    }

    @Test
    public void test2() {
        System.out.println(Arrays.toString(arr));
        heap = new Heap<>(arr, true);
        Integer[] heapArr = heap.getHeapArr();
        for (int i = 0; i < ARRAY_SIZE; i++) {
            SortUtils.swap2(heapArr, 0, heapArr.length - i - 1);
            heap.maxHeapify(heapArr, 0, heapArr.length - i - 1);
        }
        System.out.println(Arrays.toString(heap.getHeapArr()));
    }
}
