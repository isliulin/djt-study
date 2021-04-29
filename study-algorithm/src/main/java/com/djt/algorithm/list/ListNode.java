package com.djt.algorithm.list;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 链表节点
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-29
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
public class ListNode<V extends Comparable<V>> {

    /**
     * 节点值
     */
    private V value;

    /**
     * 下一个节点
     */
    private ListNode<V> next;

    public ListNode(V value) {
        this.value = value;
    }
}
