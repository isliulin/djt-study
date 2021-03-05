package com.djt.datastructure.tree;

/**
 * 树结构-接口
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
public interface Tree<N extends Node<?, ?>> {

    /**
     * 新增节点
     *
     * @param node 待插入节点
     */
    void insert(N node);

    /**
     * 删除节点
     *
     * @param node 待删除节点
     */
    void delete(N node);

    /**
     * 更新节点
     *
     * @param node 待更新节点
     */
    void update(N node);

    /**
     * 查询
     *
     * @param node 待查询节点
     * @return 节点对象
     */
    N search(N node);

}
