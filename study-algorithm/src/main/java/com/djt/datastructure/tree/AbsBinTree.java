package com.djt.datastructure.tree;

import lombok.Getter;
import lombok.Setter;

/**
 * 二叉树结构-抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-01
 */
@Setter
@Getter
public abstract class AbsBinTree<N extends AbsBinNode<?, ?>> implements Tree<N> {

    /**
     * 根节点
     */
    protected N root;

    /**
     * 节点个数
     */
    protected int size;

}
