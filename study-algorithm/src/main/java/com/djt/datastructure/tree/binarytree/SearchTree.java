package com.djt.datastructure.tree.binarytree;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 二叉搜索树
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SearchTree {

    private static final Logger LOG = LoggerFactory.getLogger(SearchTree.class);

    /**
     * 根节点
     */
    private TreeNode root;

    /**
     * 节点个数
     */
    private int size;

    /**
     * 插入节点
     *
     * @param node 待插入节点
     */
    public void insert(TreeNode node) {
        if (root == null) {
            root = node;
            ++size;
            return;
        }
        //保存当前节点
        TreeNode curNode = root;
        while (true) {
            if (node.getKey() < curNode.getKey()) {
                //左节点为空则挂上
                if (curNode.getLeft() == null) {
                    curNode.setLeft(node);
                    ++size;
                    return;
                } else {
                    //继续往左走
                    curNode = curNode.getLeft();
                }
            } else if (node.getKey() > curNode.getKey()) {
                //右节点为空则挂上
                if (curNode.getRight() == null) {
                    curNode.setRight(node);
                    ++size;
                    return;
                } else {
                    //继续往右走
                    curNode = curNode.getRight();
                }
            } else { //节点已存在 key不允许重复
                LOG.warn("该节点已存在：{}", node.getKey());
                return;
            }
        }
    }


}
