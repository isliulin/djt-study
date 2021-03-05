package com.djt.utils;

import com.djt.datastructure.tree.AbsBinNode;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 二叉树工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
public class BinaryTreeUtils {

    /**
     * 计算二叉树高度
     *
     * @param root 根节点
     * @return height
     */
    public static int getHeight(AbsBinNode<?, ?> root) {
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
     * 获取树的宽度
     *
     * @param root 根节点
     * @return width
     */
    public static int getWidth(AbsBinNode<?, ?> root) {
        return 0;
    }

    /**
     * 二叉树前序遍历
     *
     * @param root   根节点
     * @param result 遍历结果
     */
    public static <K extends Comparable<K>, V> void preOrderScan(AbsBinNode<K, V> root, List<K> result) {
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
    public static <K extends Comparable<K>, V> void inOrderScan(AbsBinNode<K, V> root, List<K> result) {
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
    public static <K extends Comparable<K>, V> void postOrderScan(AbsBinNode<K, V> root, List<K> result) {
        if (root == null) {
            return;
        }
        postOrderScan(root.getLeft(), result);
        postOrderScan(root.getRight(), result);
        result.add(root.getKey());
    }

    /**
     * 二叉树层序遍历
     * 队列方式
     *
     * @param root   根节点
     * @param result 遍历结果
     */
    public static <K extends Comparable<K>, V> void levelOrderScan(AbsBinNode<K, V> root, List<K> result) {
        if (root == null) {
            return;
        }

        //队列
        Queue<AbsBinNode<K, V>> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            //获取队头节点并访问
            AbsBinNode<K, V> curNode = queue.poll();
            result.add(curNode.getKey());
            //分别将该节点的左节点与右节点加入队列
            AbsBinNode<K, V> left = curNode.getLeft();
            if (left != null) {
                queue.offer(left);
            }
            AbsBinNode<K, V> right = curNode.getRight();
            if (right != null) {
                queue.offer(right);
            }
        }
    }

    /**
     * 二叉树层序遍历
     * 递归方式
     * 原理：按照先序遍历顺序 将每个节点的层级保存至二维数据
     * 数组下标即为层级，值即为该层的所有节点
     *
     * @param root   根节点
     * @param level  当前层级
     * @param result 遍历结果
     */
    public static <K extends Comparable<K>, V> void levelOrderScan(AbsBinNode<K, V> root, int level, List<K>[] result) {
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

    /**
     * 二叉树打印
     *
     * @param root 根节点
     */
    public static void printTree(AbsBinNode<?, ?> root) {

    }

}
