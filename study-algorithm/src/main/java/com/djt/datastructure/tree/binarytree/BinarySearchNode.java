package com.djt.datastructure.tree.binarytree;

import com.djt.datastructure.tree.AbsBinNode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 二叉树搜索树节点
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
@Setter
@Getter
@ToString(callSuper = true)
public class BinarySearchNode<K extends Comparable<K>, V> extends AbsBinNode<K, V> {

    public BinarySearchNode(K key) {
        super(key);
    }

    public BinarySearchNode(K key, V value) {
        super(key, value);
    }

}
