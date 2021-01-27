package com.djt.sort.impl;

import com.djt.sort.IArraySort;

import java.util.Arrays;

/**
 * 冒泡排序
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-22 11:15
 */
public class BubbleSort implements IArraySort {

    /*
    比较相邻的元素。如果第一个比第二个大，就交换他们两个。
    对每一对相邻元素作同样的工作，从开始第一对到结尾的最后一对。这步做完后，最后的元素会是最大的数。
    针对所有的元素重复以上的步骤，除了最后一个。
    持续每次对越来越少的元素重复上面的步骤，直到没有任何一对数字需要比较。
     */

    @Override
    public int[] sort(final int[] sourceArray) throws Exception {
        assert sourceArray != null;
        //数组拷贝，不改变参数内容
        int[] sortArr = Arrays.copyOf(sourceArray, sourceArray.length);
        for (int i = sortArr.length - 2; i >= 0; i--) {
            //是否发生交换标志
            boolean isSwap = false;
            for (int j = 0; j <= i; j++) {
                if (sortArr[j] > sortArr[j + 1]) {
                    int tmp = sortArr[j];
                    sortArr[j] = sortArr[j + 1];
                    sortArr[j + 1] = tmp;
                    isSwap = true;
                }
            }
            //若未发生交换 说明剩下元素已经有序 排序结束
            if (!isSwap) {
                break;
            }
        }

        return sortArr;
    }
}
