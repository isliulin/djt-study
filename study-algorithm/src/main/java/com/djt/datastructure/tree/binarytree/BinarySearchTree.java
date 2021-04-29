package com.djt.datastructure.tree.binarytree;

import cn.hutool.core.lang.Pair;
import com.djt.datastructure.tree.AbsBinTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 二叉搜索树
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-25
 */
public class BinarySearchTree<K extends Comparable<K>, V> extends AbsBinTree<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(BinarySearchTree.class);


    @Override
    public void insert(K key, V value) {
        BinarySearchNode<K, V> node = new BinarySearchNode<>(key, value);
        if (getRoot() == null) {
            setRoot(node);
            ++size;
            return;
        }
        //保存当前节点
        BinarySearchNode<K, V> curNode = (BinarySearchNode<K, V>) getRoot();
        while (true) {
            if (node.compareTo(curNode) < 0) {
                //左节点为空则挂上
                if (curNode.getLeft() == null) {
                    curNode.setLeft(node);
                    ++size;
                    return;
                } else {
                    //继续往左走
                    curNode = (BinarySearchNode<K, V>) curNode.getLeft();
                }
            } else if (node.compareTo(curNode) > 0) {
                //右节点为空则挂上
                if (curNode.getRight() == null) {
                    curNode.setRight(node);
                    ++size;
                    return;
                } else {
                    //继续往右走
                    curNode = (BinarySearchNode<K, V>) curNode.getRight();
                }
            } else { //节点已存在 key不允许重复
                LOG.warn("该节点已存在：{}", node.getKey());
                return;
            }
        }
    }

    @Override
    public void delete(K key) {
        if (getRoot() == null || key == null) {
            return;
        }

        //先找到该节点 及其父节点
        Pair<BinarySearchNode<K, V>, BinarySearchNode<K, V>> findPair = searchWithParent(key);
        BinarySearchNode<K, V> curNode = findPair.getKey();
        BinarySearchNode<K, V> parentNode = findPair.getValue();
        if (curNode == null) {
            LOG.info("该节点不存在：{}", key);
            return;
        }
        //判断该节点是其父节点的左还是右 0-当前节点是根节点 1-左 2-右
        byte pFlag = 0;
        if (parentNode != null) {
            pFlag = (byte) (parentNode.getLeft().compareTo(curNode) == 0 ? 1 : 2);
        }

        //删除分三种情况：1.叶子节点 2.有一个子节点 3.有两个子节点
        BinarySearchNode<K, V> left = (BinarySearchNode<K, V>) curNode.getLeft();
        BinarySearchNode<K, V> right = (BinarySearchNode<K, V>) curNode.getRight();
        //叶子节点直接删除 即将父节点指向空
        if (left == null && right == null) {
            switch (pFlag) {
                //该叶子节点是根节点
                case 0:
                    setRoot(null);
                    break;
                //该节点是父节点的左
                case 1:
                    parentNode.setLeft(null);
                    break;
                //该节点是父节点的右
                case 2:
                    parentNode.setRight(null);
                    break;
                default:
                    break;
            }
        } else if (left != null && right == null) {
            //有左节点 无右节点
            switch (pFlag) {
                //该节点是根节点 将根节点置为该节点的左子节点
                case 0:
                    setRoot(curNode.getLeft());
                    break;
                //该节点是父节点的左 将父节点的左置为该节点的左
                case 1:
                    parentNode.setLeft(curNode.getLeft());
                    break;
                //该节点是父节点的右 将父节点的右置为该节点的左
                case 2:
                    parentNode.setRight(curNode.getLeft());
                    break;
                default:
                    break;
            }
        } else if (left == null) {
            //无左节点 有右节点
            switch (pFlag) {
                //该节点是根节点 将根节点置为该节点的右子节点
                case 0:
                    setRoot(curNode.getRight());
                    break;
                //该节点是父节点的左 将父节点的左置为该节点的右
                case 1:
                    parentNode.setLeft(curNode.getRight());
                    break;
                //该节点是父节点的右 将父节点的右置为该节点的右
                case 2:
                    parentNode.setRight(curNode.getRight());
                    break;
                default:
                    break;
            }
        } else {
            //左右都有 此情况略微复杂
            //先找右子树的最小节点及其父节点
            Pair<BinarySearchNode<K, V>, BinarySearchNode<K, V>> pair = findMaxOrMinWithParent((BinarySearchNode<K, V>) curNode.getRight(), false);
            //最小节点一定不为空 且最多只有右节点
            BinarySearchNode<K, V> minNode = pair.getKey();
            BinarySearchNode<K, V> minParentNode = pair.getValue();

            //若最小节点的父节点不为空 必为其父的左节点
            if (minParentNode != null) {
                //将其父的左节点设置为最小节点的右节点 不管是否为空
                minParentNode.setLeft(minNode.getRight());
                //最小节点的右节点设置为当前节点的右节点
                minNode.setRight(curNode.getRight());
            } //反之 若最小节点的父节点为空 则说明最小节点 即为当前节点的右节点  直接进行下一步

            //设置最小节点的左节点为当前节点的左节点 此步为必须
            minNode.setLeft(curNode.getLeft());

            switch (pFlag) {
                //该节点是根节点
                case 0:
                    //将最小节点设置为根节点
                    setRoot(minNode);
                    break;
                //该节点是父节点的左
                case 1:
                    //将该节点父节点的左节点设置为最小节点
                    parentNode.setLeft(minNode);
                    break;
                //该节点是父节点的右
                case 2:
                    //将该节点父节点的右节点设置为最小节点
                    parentNode.setRight(minNode);
                    break;
                default:
                    break;
            }
        }
        //修改节点个数
        --size;
    }

    @Override
    public void update(K key, V value) {
        BinarySearchNode<K, V> findNode = searchNode(key);
        if (null == findNode) {
            insert(key, value);
        } else {
            findNode.setValue(value);
        }
    }

    @Override
    public BinarySearchNode<K, V> searchNode(K key) {
        return searchWithParent(key).getKey();
    }

    /**
     * 查找节点 附带父节点
     *
     * @param key 带查找的节点key
     * @return <子,父>
     */
    public Pair<BinarySearchNode<K, V>, BinarySearchNode<K, V>> searchWithParent(K key) {
        if (getRoot() == null || key == null) {
            return new Pair<>(null, null);
        }
        //当前节点父节点
        BinarySearchNode<K, V> parentNode = null;
        //当前节点
        BinarySearchNode<K, V> curNode = (BinarySearchNode<K, V>) getRoot();
        while (curNode != null) {
            //待查找节点与当前节点的比较结果
            int compare = key.compareTo(curNode.getKey());
            if (compare < 0) {
                //小于往左走
                parentNode = curNode;
                curNode = (BinarySearchNode<K, V>) curNode.getLeft();
            } else if (compare > 0) {
                //大于往右走
                parentNode = curNode;
                curNode = (BinarySearchNode<K, V>) curNode.getRight();
            } else {
                //等于直接返回
                return new Pair<>(curNode, parentNode);
            }
        }
        return new Pair<>(null, null);
    }

    /**
     * 获取最大节点
     *
     * @return max
     */
    public BinarySearchNode<K, V> findMax() {
        return findMaxOrMinWithParent((BinarySearchNode<K, V>) getRoot(), true).getKey();
    }

    /**
     * 获取最小节点
     *
     * @return min
     */
    public BinarySearchNode<K, V> findMin() {
        return findMaxOrMinWithParent((BinarySearchNode<K, V>) getRoot(), false).getKey();
    }

    /**
     * 找子树的最大或最小节点 附带父节点
     *
     * @param root  子树根节点
     * @param isMax 是否找最大值
     * @return <子,父>
     */
    public Pair<BinarySearchNode<K, V>, BinarySearchNode<K, V>> findMaxOrMinWithParent(BinarySearchNode<K, V> root, boolean isMax) {
        if (getRoot() == null || root == null) {
            return new Pair<>(null, null);
        }
        //当前节点父节点
        BinarySearchNode<K, V> parentNode = null;
        //当前节点
        BinarySearchNode<K, V> curNode = root;
        while (curNode != null) {
            if (isMax) {
                if (curNode.getRight() != null) {
                    parentNode = curNode;
                    curNode = (BinarySearchNode<K, V>) curNode.getRight();
                } else {
                    return new Pair<>(curNode, parentNode);
                }
            } else {
                if (curNode.getLeft() != null) {
                    parentNode = curNode;
                    curNode = (BinarySearchNode<K, V>) curNode.getLeft();
                } else {
                    return new Pair<>(curNode, parentNode);
                }
            }
        }
        return new Pair<>(null, null);
    }

}
