package com.djt.datastructure.tree;

import lombok.Getter;
import lombok.Setter;

/**
 * 二叉树结构-抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
@Setter
@Getter
public abstract class AbsBinTree<K extends Comparable<K>, V> implements Tree<K, V> {

    /**
     * 根节点
     */
    protected AbsBinNode<K, V> root;

    /**
     * 节点个数
     */
    protected int size;

    @Override
    public V searchValue(K key) {
        AbsBinNode<K, V> node = (AbsBinNode<K, V>) searchNode(key);
        if (null == node) {
            return null;
        }
        return node.getValue();
    }
}
