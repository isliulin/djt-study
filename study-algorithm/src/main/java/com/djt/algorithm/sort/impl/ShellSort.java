package com.djt.algorithm.sort.impl;

import com.djt.algorithm.sort.IArraySort;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * 希尔排序
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-14
 */
public class ShellSort implements IArraySort {

    /*
    先将整个待排元素序列分割成若干个子序列（由相隔某个“增量”的元素组成的）分别进行直接插入排序，
    然后依次缩减增量再进行排序，待整个序列中的元素基本有序（增量足够小）时，再对全体元素进行一次直接插入排序。
     */

    @Override
    public int[] sort(final int[] sourceArray) {
        Validate.notNull(sourceArray);
        int[] sortArr = Arrays.copyOf(sourceArray, sourceArray.length);

        int gap = sortArr.length / 2 + 1;
        while (gap > 0) {
            for (int i = gap; i < sortArr.length; i += gap) {
                int tmp = sortArr[i];
                int j = i - gap;
                while (j >= 0 && sortArr[j] > tmp) {
                    sortArr[j + gap] = sortArr[j];
                    j -= gap;
                }
                sortArr[j + gap] = tmp;
            }
            gap /= 2;
        }

        return sortArr;
    }

    @Override
    public String getName() {
        return "希尔排序";
    }

}
