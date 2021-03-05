package com.djt.datastructure.tree;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 二叉树节点-抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
@Setter
@Getter
@ToString
public abstract class AbsBinNode<K extends Comparable<K>, V> implements Node<K, V>, Comparable<AbsBinNode<K, V>> {

    /**
     * 节点key 用于标识与比较
     */
    protected K key;

    /**
     * 节点保存的数据
     */
    protected V value;

    /**
     * 左节点
     */
    protected AbsBinNode<K, V> left;

    /**
     * 右节点
     */
    protected AbsBinNode<K, V> right;

    public AbsBinNode(K key) {
        this.key = key;
    }

    public AbsBinNode(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean isLeaf() {
        return left == null && right == null;
    }

    @Override
    public int compareTo(AbsBinNode<K, V> o) {
        return key.compareTo(o.key);
    }
}
