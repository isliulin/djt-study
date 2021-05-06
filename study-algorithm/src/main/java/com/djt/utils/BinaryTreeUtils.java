package com.djt.utils;

import com.djt.datastructure.tree.AbsBinNode;
import com.djt.datastructure.tree.AbsBinTree;
import com.djt.enums.TreeEnums;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;

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


    public static <K extends Comparable<K>, V> List<Pair<K, Integer>>[] levelCollect(AbsBinNode<K, V> root) {
        if (root == null) {
            return null;
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
        initStrArr(printArrs, " ");
        //坐标平移距离
        int move = width / 2;


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
            printArrs[lineIdx][colIdx] = node.getKey().toString();

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
        return listArr;
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

    public static <K extends Comparable<K>, V> void treePrint(AbsBinNode<K, V> root) {
        List<Pair<K, Integer>>[] lisrArr = levelCollect(root);
        for (int i = 0; i < lisrArr.length; i++) {
            List<Pair<K, Integer>> list = lisrArr[i];
            StringBuilder sb = new StringBuilder();
            for (Pair<K, Integer> pair : list) {
                sb.append(pair.getKey()).append(":").append(pair.getValue()).append(",");
            }
            System.out.println("第 " + (i + 1) + " 层：" + sb);
        }
    }


    /**
     * 打印二叉树
     * 队列方式 结果保存 key + 层级
     *
     * @param binTree 二叉树对象
     */
    public static <K extends Comparable<K>> void levelPrint(AbsBinTree<K, ?> binTree) {
        if (binTree == null) {
            return;
        }
        AbsBinNode<K, ?> root = binTree.getRoot();

        //创建层序遍历队列 队列元素内容：<节点, 层级, <父节点key,左右>>
        Queue<Triple<AbsBinNode<K, ?>, Integer, Pair<K, TreeEnums.ChildSide>>> queue = new LinkedList<>();
        queue.offer(Triple.of(root, 1, Pair.of(null, null)));

        int height = getHeight(root);
        //节点的元数据 List元素个数与层级相同 key：key  + 中点距离 + 父节点key + 左右子key
        List<Map<K, TwoWayBinNode<K>>> nodeMetaList = new ArrayList<>(height);
        for (int i = 0; i < height; i++) {
            int size = (int) Math.pow(2, i);
            HashMap<K, TwoWayBinNode<K>> lineMap = new HashMap<>(size);
            if (i == 0) {
                lineMap.put(root.getKey(), new TwoWayBinNode<>(root.getKey(), 0));
            }
            nodeMetaList.add(i, lineMap);
        }

        //层序遍历 动态调整
        while (!queue.isEmpty()) {
            //出队头节点
            Triple<AbsBinNode<K, ?>, Integer, Pair<K, TreeEnums.ChildSide>> triple = queue.poll();
            AbsBinNode<K, ?> curNode = triple.getLeft();
            int curLevel = triple.getMiddle();
            Pair<K, TreeEnums.ChildSide> parent = triple.getRight();
            K parentKey = parent.getLeft();
            TreeEnums.ChildSide childSide = parent.getRight();

            //计算当前节点中点距离
            int curMidLen = 0;
            TwoWayBinNode<K> parentMetaNode = null;
            //当前节点非根节点
            if (parentKey != null && curLevel > 1) {
                //获取父节点所在行的元数据
                Map<K, TwoWayBinNode<K>> parentMetaMap = nodeMetaList.get(curLevel - 2);
                parentMetaNode = parentMetaMap.get(parentKey);
                int parentMidLen = parentMetaNode.getMidLen();
                //此处需要区分自己是父节点的左还是右 左=p-1 右=p+1
                if (childSide == TreeEnums.ChildSide.LEFT) {
                    curMidLen = parentMidLen - 1;
                } else {
                    curMidLen = parentMidLen + 1;
                }
            }

            //获取当前行元数据 取中点距离的最大值
            Map<K, TwoWayBinNode<K>> curMetaMap = nodeMetaList.get(curLevel - 1);
            int maxMidLen = curMetaMap.values().stream().map(TwoWayBinNode::getMidLen).max(Integer::compareTo).orElse(Integer.MIN_VALUE);
            //判断当前位置是否被占 未被占则正常打印 被占则向上调整
            //节点节点之间至少相隔一个位置
            if (curMidLen > maxMidLen + 1) {
                curMetaMap.put(curNode.getKey(), new TwoWayBinNode<>(curNode.getKey(), curMidLen, parentMetaNode));
            } else {
                /*
                 * 向上调整逻辑： 主要是对 nodeMetaList 进行调整
                 * 当前节点：C  父节点：P  父节点左子树：P-L  父节点左子树：P-R
                 * 若P存在，则C一定是P-L，且P-R一定未被打印
                 * 1. P+1 ：父节点向右移动一个距离，无需关心P-R，因为其尚未打印
                 * 2. PP-L-1 ：即叔叔节点向左扩展
                 * 3. 判断
                 *
                 */
            }

            //分别将该节点的左节点与右节点加入队列
            AbsBinNode<K, ?> left = curNode.getLeft();
            if (left != null) {
                queue.offer(Triple.of(left, curLevel + 1, Pair.of(curNode.getKey(), TreeEnums.ChildSide.LEFT)));
            }
            AbsBinNode<K, ?> right = curNode.getRight();
            if (right != null) {
                queue.offer(Triple.of(right, curLevel + 1, Pair.of(curNode.getKey(), TreeEnums.ChildSide.RIGHT)));
            }
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

    /**
     * 打印时用双向节点
     */
    @Setter
    @Getter
    public static class TwoWayBinNode<K extends Comparable<K>> {

        /**
         * 节点key
         */
        private K key;

        /**
         * 中点距离
         */
        private int midLen;

        /**
         * 父节点
         */
        private TwoWayBinNode<K> parent;

        /**
         * 左子节点
         */
        private TwoWayBinNode<K> left;

        /**
         * 右子节点
         */
        private TwoWayBinNode<K> right;

        public TwoWayBinNode(K key, int midLen) {
            this.key = key;
            this.midLen = midLen;
        }

        public TwoWayBinNode(K key, int midLen, TwoWayBinNode<K> parent) {
            this.key = key;
            this.midLen = midLen;
            this.parent = parent;
        }

        /**
         * 判断当前节点是其父节点的左或右
         *
         * @return left/right/null
         */
        public TreeEnums.ChildSide getSide() {
            if (parent == null) {
                return null;
            }
            if (key.equals(parent.left.key)) {
                return TreeEnums.ChildSide.LEFT;
            }
            if (key.equals(parent.right.key)) {
                return TreeEnums.ChildSide.RIGHT;
            }
            return null;
        }

    }


}
