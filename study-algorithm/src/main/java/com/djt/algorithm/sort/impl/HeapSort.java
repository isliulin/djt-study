package com.djt.algorithm.sort.impl;

import com.djt.algorithm.heap.Heap;
import com.djt.algorithm.sort.IArraySort;
import com.djt.utils.SortUtils;

import java.util.Arrays;

/**
 * 堆排序
 *
 * @author 　djt317@qq.com
 * @since 　 2021-05-10
 */
public class HeapSort implements IArraySort {

    @Override
    public int[] sort(int[] sourceArray) {
        Integer[] arr = Arrays.stream(sourceArray).boxed().toArray(Integer[]::new);
        Heap<Integer> heap = new Heap<>(arr, true);
        Integer[] heapArr = heap.getHeapArr();
        for (int i = 0; i < heapArr.length; i++) {
            SortUtils.swap2(heapArr, 0, heapArr.length - i - 1);
            heap.maxHeapify(heapArr, 0, heapArr.length - i - 1);
        }
        return Arrays.stream(heapArr).mapToInt(Integer::intValue).toArray();
    }

    @Override
    public String getName() {
        return "堆排序";
    }
}
