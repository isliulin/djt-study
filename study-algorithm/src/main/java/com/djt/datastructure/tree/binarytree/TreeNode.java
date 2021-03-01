package com.djt.datastructure.tree.binarytree;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 二叉树节点
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
@Setter
@Getter
@ToString
public class TreeNode {

    /**
     * 节点key 用于标识与比较
     */
    private int key;

    /**
     * 节点保存的数据
     */
    private String value;

    /**
     * 左子节点
     */
    private TreeNode left;

    /**
     * 右子节点
     */
    private TreeNode right;

    public TreeNode(int key) {
        this.key = key;
    }

    /**
     * 判断是否叶子节点
     *
     * @return true-叶子节点 false-非叶子节点
     */
    public boolean isLeaf() {
        return left == null && right == null;
    }
}
