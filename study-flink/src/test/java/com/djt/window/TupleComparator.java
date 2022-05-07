package com.djt.window;

import cn.hutool.core.comparator.CompareUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 序列化的Tuple比较器
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-22
 */
public class TupleComparator<T extends Tuple> implements Comparator<T>, Serializable {

    /**
     * 待比较的属性位置
     */
    private final int comparePos;

    /**
     * 属性比较器(若待比较的属性实现了Comparable接口,则该比较器可为空,否则会报错)
     */
    private final Comparator<?> comparator;

    public TupleComparator(int comparePos) {
        this(comparePos, null);
    }

    public TupleComparator(int comparePos, Comparator<?> comparator) {
        Validate.inclusiveBetween(0, T.MAX_ARITY - 1, comparePos,
                StrUtil.format("非法的Tuple位置:{}, 只允许[0-{}].", comparePos, T.MAX_ARITY - 1));
        this.comparePos = comparePos;
        this.comparator = comparator;
    }

    @Override
    public int compare(T o1, T o2) {
        if (o1.getArity() <= comparePos) {
            throw new IndexOutOfBoundsException(
                    StrUtil.format("访问元组位置越界,Tuple元素个数:{},访问位置:{}", o1.getArity(), comparePos));
        }
        return CompareUtil.compare(o1.getField(comparePos), o2.getField(comparePos), comparator);
    }
}
