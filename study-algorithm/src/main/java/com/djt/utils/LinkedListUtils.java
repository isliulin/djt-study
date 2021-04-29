package com.djt.utils;

import cn.hutool.core.text.StrBuilder;
import com.djt.algorithm.list.LinkedList;
import com.djt.algorithm.list.ListNode;

/**
 * 链表工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-29
 */
public class LinkedListUtils {

    /**
     * 链表打印
     *
     * @param list list
     */
    public static void printList(LinkedList<?> list) {
        if (null == list || list.isEmpty()) {
            return;
        }
        StrBuilder sb = new StrBuilder("[ ");
        ListNode<?> curNode = list.getHead();
        while (curNode != null) {
            sb.append(curNode.getValue());
            curNode = curNode.getNext();
            if (curNode != null) {
                sb.append(" -> ");
            }
        }
        sb.append(" ]");
        System.out.println(sb.toString());
    }


}
