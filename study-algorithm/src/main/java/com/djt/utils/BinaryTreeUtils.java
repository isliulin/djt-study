package com.djt.utils;

import com.djt.datastructure.tree.AbsBinNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

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

    public static final String[] CHAR_ARR = {"┏", "━", "┓", "┃"};

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


    public static <K extends Comparable<K>, V> void levelCollect(AbsBinNode<K, V> root) {
        if (root == null) {
            return;
        }

        //获取树高度
        int height = BinaryTreeUtils.getHeight(root);
        List<Pair<K, Integer>>[] listArr = new ArrayList[height];
        for (int i = 0; i < listArr.length; i++) {
            listArr[i] = new ArrayList<>();
        }

        //获取树宽度
        int width = (int) Math.pow(2, height) - 1;
        //创建打印二维数组 height*2行 width列
        String[][] printArrs = new String[height * 2][width];
        initStrArr(printArrs, "  ");
        //坐标平移距离
        int move = width / 2;

        int numWid = 2;

        //队列 <节点,层级,坐标>
        Queue<Triple<AbsBinNode<K, V>, Integer, Integer>> queue = new LinkedList<>();
        queue.offer(Triple.of(root, 1, 0));
        while (!queue.isEmpty()) {
            //出队头节点
            Triple<AbsBinNode<K, V>, Integer, Integer> curTriple = queue.poll();
            AbsBinNode<K, V> node = curTriple.getLeft();
            int level = curTriple.getMiddle();
            int index = curTriple.getRight();

            int lineIdx = level * 2 - 2;
            int colIdx = index + move;
            printArrs[lineIdx][colIdx] = StringUtils.rightPad(node.getKey().toString(), numWid, " ");

            listArr[level - 1].add(Pair.of(node.getKey(), index));

            int dis = (int) Math.pow(2, (height - 2 - (level - 1)));

            //分别将该节点的左节点与右节点加入队列
            AbsBinNode<K, V> left = node.getLeft();
            if (left != null) {
                queue.offer(Triple.of(left, level + 1, index - dis));
                for (int i = 1; i < dis; i++) {
                    printArrs[lineIdx][colIdx - i] = CHAR_ARR[1];
                }
                printArrs[lineIdx][colIdx - dis] = CHAR_ARR[0];
                printArrs[lineIdx + 1][colIdx - dis] = CHAR_ARR[3];
            }
            AbsBinNode<K, V> right = node.getRight();
            if (right != null) {
                queue.offer(Triple.of(right, level + 1, index + dis));
                for (int i = 1; i < dis; i++) {
                    printArrs[lineIdx][colIdx + i] = CHAR_ARR[1];
                }
                printArrs[lineIdx][colIdx + dis] = CHAR_ARR[2];

                printArrs[lineIdx + 1][colIdx + dis] = CHAR_ARR[3];
            }
        }
        printArrs(printArrs);
        printPairList(listArr);
    }

    public static void printArrs(String[][] arrs) {
        if (arrs == null) {
            return;
        }
        for (String[] arr : arrs) {
            for (String s : arr) {
                System.out.print(s);
            }
            System.out.println();
        }
    }

    public static <K extends Comparable<K>> void printPairList(List<Pair<K, Integer>>[] result) {
        for (int i = 0; i < result.length; i++) {
            List<Pair<K, Integer>> list = result[i];
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < list.size(); j++) {
                Pair<K, Integer> pair = list.get(j);
                sb.append(pair.getKey()).append(":").append(pair.getValue());
                if (j < list.size() - 1) {
                    sb.append(",");
                }
            }
            System.out.println("第 " + (i + 1) + " 层：" + sb);
        }
    }

    /**
     * 二维字符数组初始化
     *
     * @param charsArr 二维字符数组
     * @param c        始化值
     */
    public static void initCharArr(char[][] charsArr, char c) {
        for (int i = 0; i < charsArr.length; i++) {
            char[] charArr = charsArr[i];
            for (int j = 0; j < charArr.length; j++) {
                charsArr[i][j] = c;
            }
        }
    }

    public static void initStrArr(String[][] charsArr, String c) {
        for (int i = 0; i < charsArr.length; i++) {
            String[] charArr = charsArr[i];
            for (int j = 0; j < charArr.length; j++) {
                charsArr[i][j] = c;
            }
        }
    }

}
