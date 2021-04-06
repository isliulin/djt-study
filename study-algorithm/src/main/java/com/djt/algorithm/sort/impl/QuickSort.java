package com.djt.algorithm.sort.impl;

import com.djt.algorithm.sort.IArraySort;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * 快速排序
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-06
 */
public class QuickSort implements IArraySort {


    @Override
    public int[] sort(int[] sourceArray) throws Exception {
        Validate.notNull(sourceArray);
        int[] sortArr = Arrays.copyOf(sourceArray, sourceArray.length);
        sort(sortArr, 0, sortArr.length - 1);
        return sortArr;
    }

    private void sort(int[] array, int left, int right) {
        Validate.notNull(array);
        if (left >= right) {
            return;
        }

        int pivotloc = partition(array, left, right);
        sort(array, left, pivotloc - 1);
        sort(array, pivotloc + 1, right);
    }

    /**
     * 划分区间
     *
     * @param array 原数组
     * @param left  起始下标
     * @param right 终止下标
     * @return 基数所在位置
     */
    private int partition(int[] array, int left, int right) {
        Validate.isTrue(left <= right, "非法下标！");

        int base = array[left];
        while (left < right) {
            while (left < right && array[right] >= base) {
                --right;
            }
            array[left] = array[right];
            while (left < right && array[left] <= base) {
                ++left;
            }
            array[right] = array[left];
        }
        array[left] = base;
        return left;
    }
}
