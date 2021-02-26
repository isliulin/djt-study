package com.djt.utils;

import com.djt.datastructure.tree.binarytree.TreeNode;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

/**
 * 树工具类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-25 16:02
 */
public class TreeUtils {

    /**
     * 计算树高度
     *
     * @param root 根节点
     * @return height
     */
    public static int getHeight(TreeNode root) {
        if (root == null) {
            return 0;
        }

        //递归分别求左右子树的高度
        int leftHight = getHeight(root.getLeft());
        int rightHight = getHeight(root.getRight());

        //当前节点的高度 = MAX(左子树高度，右子树高度) + 1
        return Math.max(leftHight, rightHight) + 1;
    }

    /**
     * 二叉树前序遍历
     *
     * @param root   根节点
     * @param result 遍历结果
     */
    public static void preOrderScan(TreeNode root, List<Integer> result) {
        if (root == null) {
            return;
        }
        //访问当前节点
        result.add(root.getKey());
        //递归访问左子树
        preOrderScan(root.getLeft(), result);
        //递归访问右子树
        preOrderScan(root.getRight(), result);
    }

    /**
     * 二叉树中序遍历
     *
     * @param root   根节点
     * @param result 遍历结果
     */
    public static void inOrderScan(TreeNode root, List<Integer> result) {
        if (root == null) {
            return;
        }
        //递归访问左子树
        inOrderScan(root.getLeft(), result);
        //访问当前节点
        result.add(root.getKey());
        //递归访问右子树
        inOrderScan(root.getRight(), result);
    }

    /**
     * 二叉树后序遍历
     *
     * @param root   根节点
     * @param result 遍历结果
     */
    public static void postOrderScan(TreeNode root, List<Integer> result) {
        if (root == null) {
            return;
        }
        postOrderScan(root.getLeft(), result);
        postOrderScan(root.getRight(), result);
        result.add(root.getKey());
    }


    /**
     * 二叉树层序遍历
     * 原理：按照先序遍历顺序 将每个节点的层级保存至二维数据
     * 数组下标即为层级，值即为该层的所有节点
     *
     * @param root   根节点
     * @param level  当前层级
     * @param result 遍历结果
     */
    public static void levelOrderScan(TreeNode root, int level, List<Integer>[] result) {
        if (root == null) {
            return;
        }
        Validate.isTrue(level > 0, "错误：level<=0");

        int index = level - 1;
        if (result[index] == null) {
            result[index] = new ArrayList<>();
        }
        result[index].add(root.getKey());

        levelOrderScan(root.getLeft(), level + 1, result);
        levelOrderScan(root.getRight(), level + 1, result);
    }


}
