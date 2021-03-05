package com.djt.datastructure.tree;

/**
 * 树节点-接口
 * K-key类型 V-value类型
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
public interface Node<K, V> {

    /**
     * 获取key
     *
     * @return key
     */
    K getKey();

    /**
     * 获取值
     *
     * @return value
     */
    V getValue();

    /**
     * 判断是否叶子节点
     *
     * @return true-叶子节点 false-非叶子节点
     */
    boolean isLeaf();

}
