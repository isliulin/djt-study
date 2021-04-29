package com.djt.datastructure.tree;

/**
 * 树结构-接口
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
public interface Tree<K, V> {

    /**
     * 新增节点
     *
     * @param key   待插入节点key
     * @param value 待插入节点value
     */
    void insert(K key, V value);

    /**
     * 删除节点
     *
     * @param key 待删除节点key
     */
    void delete(K key);

    /**
     * 更新节点
     *
     * @param key   待更新节点key
     * @param value 待更新节点value
     */
    void update(K key, V value);

    /**
     * 查询节点对象
     *
     * @param key 待查询节点key
     * @return 节点对象
     */
    Node<K, V> searchNode(K key);

    /**
     * 查询节点值
     *
     * @param key 待查询节点key
     * @return 节点值
     */
    V searchValue(K key);

}
