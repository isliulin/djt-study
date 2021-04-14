package com.djt.algorithm.sort.impl;

import com.djt.algorithm.sort.IArraySort;
import com.djt.utils.SortUtils;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * 选择排序
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-27
 */
public class SelectSort implements IArraySort {

    /*
     首先在未排序序列中找到最小（大）元素，存放到排序序列的起始位置
     再从剩余未排序元素中继续寻找最小（大）元素，然后放到已排序序列的末尾。
     重复第二步，直到所有元素均排序完毕。
     */

    @Override
    public int[] sort(final int[] sourceArray) {
        Validate.notNull(sourceArray);
        int[] sortArr = Arrays.copyOf(sourceArray, sourceArray.length);

        for (int i = 0; i < sortArr.length - 1; i++) {
            int minIdx = i;
            for (int j = i + 1; j < sortArr.length; j++) {
                if (sortArr[j] < sortArr[minIdx]) {
                    minIdx = j;
                }
            }
            //非自身 则交换
            if (minIdx != i) {
                SortUtils.swap(sortArr, i, minIdx);
            }
        }

        return sortArr;
    }

    @Override
    public String getName() {
        return "选择排序";
    }

}
