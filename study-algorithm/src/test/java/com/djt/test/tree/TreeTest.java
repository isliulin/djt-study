package com.djt.test.tree;

import cn.hutool.core.bean.BeanUtil;
import com.djt.datastructure.tree.binarytree.BinarySearchNode;
import com.djt.datastructure.tree.binarytree.BinarySearchTree;
import com.djt.utils.BinaryTreeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 树测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
public class TreeTest {

    private BinarySearchTree<Integer, Integer> binarySearchTree = null;

    @Before
    public void before() {
        resetTree();
    }

    /**
     * 生成节点列表
     *
     * @return list
     */
    public List<Integer> getNodeList() {
        List<Integer> nodeList = new ArrayList<>();
        nodeList.add(30);
        nodeList.add(15);
        nodeList.add(7);
        nodeList.add(5);
        nodeList.add(10);
        nodeList.add(20);
        nodeList.add(18);
        nodeList.add(25);
        nodeList.add(45);
        nodeList.add(40);
        nodeList.add(35);
        nodeList.add(42);
        nodeList.add(55);
        nodeList.add(50);
        nodeList.add(60);
        return nodeList;
    }

    /**
     * 重建二叉树
     */
    public void resetTree() {
        binarySearchTree = new BinarySearchTree<>();
        for (int num : getNodeList()) {
            binarySearchTree.insert(num, num);
        }
    }

    @Test
    public void testGetHeight() {
        System.out.println(BinaryTreeUtils.getHeight(binarySearchTree.getRoot()));
        System.out.println(BinaryTreeUtils.getHeight(binarySearchTree.getRoot().getLeft()));
        System.out.println(BinaryTreeUtils.getHeight(binarySearchTree.getRoot().getRight()));
    }

    @Test
    public void testSearchTree() {
        List<Integer> result = new ArrayList<>();
        BinaryTreeUtils.preOrderScan(binarySearchTree.getRoot(), result);
        System.out.println("前序遍历" + result);
        result.clear();
        BinaryTreeUtils.inOrderScan(binarySearchTree.getRoot(), result);
        System.out.println("中序遍历" + result);
        result.clear();
        BinaryTreeUtils.postOrderScan(binarySearchTree.getRoot(), result);
        System.out.println("后序遍历" + result);

        result.clear();
        BinaryTreeUtils.levelOrderScan(binarySearchTree.getRoot(), result);
        System.out.println("层序遍历" + result);
    }

    @Test
    public void testSearch() {
        //查找已存在的
        BinarySearchNode<Integer, Integer> find = binarySearchTree.searchNode(26);
        System.out.println(find);

        //查找不存在的
        find = binarySearchTree.searchNode(6);
        System.out.println(find);

        //更新不存在的
        binarySearchTree.update(6, 66);
        find = binarySearchTree.searchNode(6);
        System.out.println(find);

        //更新已存在的
        binarySearchTree.update(6, 666);
        find = binarySearchTree.searchNode(6);
        System.out.println(find);

        int value = binarySearchTree.searchValue(6);
        System.out.println(value);
    }

    @Test
    public void testFindMaxOrMin() {
        System.out.println(binarySearchTree.findMax().getKey());
        System.out.println(binarySearchTree.findMin().getKey());

        //先找到一颗子树
        BinarySearchNode<Integer, Integer> node = binarySearchTree.searchNode(21);
        //取子树最大最小值
        System.out.println(binarySearchTree.findMaxOrMinWithParent(node, true).getKey().getKey());
        System.out.println(binarySearchTree.findMaxOrMinWithParent(node, false).getKey().getKey());
    }

    @Test
    public void testDelete1() {
        printLevelTree();
        deleteNode(23, true);
        deleteNode(19, true);
        deleteNode(15, true);
        deleteNode(40, true);
        deleteNode(30, true);
    }

    @Test
    public void testDelete2() {
        printLevelTree();
        while (binarySearchTree.getSize() > 0) {
            deleteNode(binarySearchTree.getRoot().getKey(), false);
        }
    }

    private void deleteNode(int key, boolean isResetTree) {
        if (isResetTree) resetTree();
        System.out.println("==================删除节点" + key);
        binarySearchTree.delete(key);
        printLevelTree();
    }


    @Test
    public void testBean() {
        BinarySearchNode<Integer, Integer> node1 = new BinarySearchNode<>(30);
        BinarySearchNode<Integer, Integer> node2 = new BinarySearchNode<>(10);
        System.out.println(node1 + " " + node2);
        BeanUtil.copyProperties(node1, node2, false);
        System.out.println(node1 + " " + node2);
        node1.setKey(6);
        System.out.println(node1 + " " + node2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void printLevelTree() {
        int height = BinaryTreeUtils.getHeight(binarySearchTree.getRoot());
        List<Integer>[] result2 = new ArrayList[height];
        BinaryTreeUtils.levelOrderScan(binarySearchTree.getRoot(), 1, result2);
        for (int i = 0; i < result2.length; i++) {
            System.out.println("第 " + (i + 1) + " 层：" + result2[i]);
        }
    }

    @Test
    public void printLevelTree2() {
        List<Pair<Integer, Integer>>[] result = BinaryTreeUtils.levelCollect(binarySearchTree.getRoot());
        for (int i = 0; i < result.length; i++) {
            List<Pair<Integer, Integer>> list = result[i];
            StringBuilder sb = new StringBuilder();
            for (Pair<Integer, Integer> pair : list) {
                sb.append(pair.getKey()).append(":").append(pair.getValue()).append(",");
            }
            System.out.println("第 " + (i + 1) + " 层：" + sb);
        }
    }

    @Test
    public void testTwoWayBinNode() {
        BinaryTreeUtils.TwoWayBinNode<Integer> node1 = new BinaryTreeUtils.TwoWayBinNode<>(1, 0);
        BinaryTreeUtils.TwoWayBinNode<Integer> node2 = new BinaryTreeUtils.TwoWayBinNode<>(2, 0, node1);
        BinaryTreeUtils.TwoWayBinNode<Integer> node3 = new BinaryTreeUtils.TwoWayBinNode<>(3, 0, node1);
        node1.setLeft(node2);
        node1.setRight(node3);
        System.out.println(node1.getSide());
        System.out.println(node2.getSide());
        System.out.println(node3.getSide());
    }

    @Test
    public void testLevelPrint() {
        BinaryTreeUtils.levelPrint(binarySearchTree);
        char[][] charsArr = new char[2][3];
        BinaryTreeUtils.initCharArr(charsArr, ' ');
        System.out.println(Arrays.deepToString(charsArr));
    }


}
