package com.djt.test.tree;

import com.djt.datastructure.tree.binarytree.SearchTree;
import com.djt.datastructure.tree.binarytree.TreeNode;
import com.djt.utils.TreeUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 树测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
public class TreeTest {

    private final List<TreeNode> nodeList = new ArrayList<>();
    private final SearchTree searchTree = new SearchTree();

    @Before
    public void before() {
        nodeList.add(new TreeNode(30));
        nodeList.add(new TreeNode(15));
        nodeList.add(new TreeNode(7));
        nodeList.add(new TreeNode(21));
        nodeList.add(new TreeNode(19));
        nodeList.add(new TreeNode(20));
        nodeList.add(new TreeNode(26));
        nodeList.add(new TreeNode(23));
        nodeList.add(new TreeNode(29));
        nodeList.add(new TreeNode(40));
        nodeList.add(new TreeNode(35));
        for (TreeNode node : nodeList) {
            searchTree.insert(node);
        }
    }

    @Test
    public void testGetHeight() {
        System.out.println(TreeUtils.getHeight(searchTree.getRoot()));
        System.out.println(TreeUtils.getHeight(searchTree.getRoot().getLeft()));
        System.out.println(TreeUtils.getHeight(searchTree.getRoot().getRight()));
    }

    @Test
    public void testSearchTree() {
        List<Integer> result = new ArrayList<>();
        TreeUtils.preOrderScan(searchTree.getRoot(), result);
        System.out.println("前序遍历" + result);
        result.clear();
        TreeUtils.inOrderScan(searchTree.getRoot(), result);
        System.out.println("中序遍历" + result);
        result.clear();
        TreeUtils.postOrderScan(searchTree.getRoot(), result);
        System.out.println("后序遍历" + result);

        result.clear();
        TreeUtils.levelOrderScan(searchTree.getRoot(), result);
        System.out.println("层序遍历" + result);

        //层序遍历
        int height = TreeUtils.getHeight(searchTree.getRoot());
        List<Integer>[] result2 = new ArrayList[height];
        TreeUtils.levelOrderScan(searchTree.getRoot(), 1, result2);
        for (int i = 0; i < result2.length; i++) {
            System.out.println("第 " + i + " 层：" + result2[i]);
        }
    }
}
