package com.djt.algorithm.sort.impl;

import com.djt.algorithm.sort.IArraySort;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * 归并排序
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-15
 */
public class MergeSort implements IArraySort {

    /*
      将数据进行区间分割，递归一直到不可分割，再将两部分进行有序合并
     */

    @Override
    public int[] sort(final int[] sourceArray) {
        Validate.notNull(sourceArray);
        int[] sortArr = Arrays.copyOf(sourceArray, sourceArray.length);

        int[] tmpArr = new int[sortArr.length];
        sort(sortArr, tmpArr, 0, sortArr.length - 1);
        return sortArr;
    }

    private void sort(int[] sourceArray, int[] resultArr, int left, int right) {
        if (left >= right) {
            return;
        }

        int mid = (left + right) / 2;
        sort(sourceArray, resultArr, left, mid);
        sort(sourceArray, resultArr, mid + 1, right);
        merge(sourceArray, resultArr, left, right);
    }

    private void merge(int[] sourceArray, int[] resultArr, int left, int right) {
        if (left >= right) {
            return;
        }

        int mid = (left + right) / 2;
        int i = left;
        int j = mid + 1;
        int idx = 0;
        while (i <= mid && j <= right) {
            if (sourceArray[i] <= sourceArray[j]) {
                resultArr[idx++] = sourceArray[i++];
            } else {
                resultArr[idx++] = sourceArray[j++];
            }
        }

        while (i <= mid) {
            resultArr[idx++] = sourceArray[i++];
        }

        while (j <= right) {
            resultArr[idx++] = sourceArray[j++];
        }

        System.arraycopy(resultArr, 0, sourceArray, left, right + 1 - left);
    }


    @Override
    public String getName() {
        return "归并排序";
    }

}
