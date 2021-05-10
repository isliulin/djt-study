package com.djt.algorithm.heap;

import com.djt.utils.SortUtils;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Arrays;

/**
 * 堆
 *
 * @author 　djt317@qq.com
 * @since 　 2021-05-10
 */
@Getter
@ToString
@NoArgsConstructor
public class Heap<T extends Comparable<T>> {

    /**
     * 是否是大根堆 默认小根堆
     */
    private boolean isBigHeap = false;

    /**
     * 堆数组
     */
    private T[] heapArr = null;

    /**
     * @param arr       原数组
     * @param isBigHeap 是否是大根堆
     */
    public Heap(T[] arr, boolean isBigHeap) {
        //从最后一个有子节点的节点开始构建
        this.isBigHeap = isBigHeap;
        this.heapArr = Arrays.copyOf(arr, arr.length);
        int lengthParent = parent(heapArr.length - 1);
        for (int i = lengthParent; i >= 0; i--) {
            if (isBigHeap) {
                maxHeapify(heapArr, i, heapArr.length);
            } else {
                minHeapify(heapArr, i, heapArr.length);
            }
        }

    }

    /**
     * 从节点i开始进行最大化调整
     *
     * @param arr 数组
     * @param i   待调整的节点
     * @param len 数组长度 不一定是整个数组
     */
    public void maxHeapify(T[] arr, int i, int len) {
        int left = left(i);
        int right = right(i);
        int maxIdx;

        //选出三者最大值
        if (left < len && arr[i].compareTo(arr[left]) < 0) {
            maxIdx = left;
        } else {
            maxIdx = i;
        }

        if (right < len && arr[maxIdx].compareTo(arr[right]) < 0) {
            maxIdx = right;
        }

        //最大值不等于自身 则需要递归调整
        if (maxIdx != i) {
            SortUtils.swap2(arr, maxIdx, i);
            maxHeapify(arr, maxIdx, len);
        }
    }

    /**
     * 从节点i开始进行最小化调整
     *
     * @param arr 数组
     * @param i   待调整的节点
     * @param len 数组长度 不一定是整个数组
     */
    public void minHeapify(T[] arr, int i, int len) {
        int left = left(i);
        int right = right(i);
        int minIdx;

        //选出三者最小值
        if (left < len && arr[i].compareTo(arr[left]) > 0) {
            minIdx = left;
        } else {
            minIdx = i;
        }

        if (right < len && arr[minIdx].compareTo(arr[right]) > 0) {
            minIdx = right;
        }

        //最小值不等于自身 则需要递归调整
        if (minIdx != i) {
            SortUtils.swap2(arr, minIdx, i);
            minHeapify(arr, minIdx, len);
        }
    }

    /**
     * 获取左子节点下标
     *
     * @param i 当前节点
     * @return index
     */
    public int left(int i) {
        return i * 2 + 1;
    }

    /**
     * 获取右子节点下标
     *
     * @param i 当前节点
     * @return index
     */
    public int right(int i) {
        return i * 2 + 2;
    }

    /**
     * 获取父节点下标
     *
     * @param i 当前节点
     * @return index
     */
    public int parent(int i) {
        if (i == 0) {
            return -1;
        }
        return (i - 1) / 2;
    }


}
