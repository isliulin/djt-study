package com.djt.utils;

import org.apache.commons.lang3.Validate;

/**
 * 排序工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-28
 */
public class SortUtils {

    /**
     * 数组元素交换
     *
     * @param array 数组
     * @param left  下标1
     * @param right 下标2
     */
    public static void swap(int[] array, int left, int right) {
        Validate.notNull(array);
        int tmp = array[left];
        array[left] = array[right];
        array[right] = tmp;
    }
}
