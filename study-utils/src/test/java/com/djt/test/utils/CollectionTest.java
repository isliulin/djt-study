package com.djt.test.utils;

import cn.hutool.core.collection.CollectionUtil;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-02-15
 */
public class CollectionTest {

    @Test
    public void test1() {
        Set<String> set1 = new HashSet<>();
        set1.add("a");
        set1.add("b");
        set1.add("c");
        Set<String> set2 = new HashSet<>();
        set2.add("b");
        set2.add("c");
        set2.add("d");
        //交集
        System.out.println(CollectionUtil.intersection(set1, set2));
        //差集
        System.out.println(CollectionUtil.disjunction(set1, set2));
        //单差集 集合1有且集合2没有的
        System.out.println(CollectionUtil.subtract(set1, set2));
    }

    @Test
    public void test2() {
        Set<String> oldSet = new HashSet<>();
        oldSet.add("a");
        oldSet.add("b");
        oldSet.add("c");
        oldSet.add("d");
        oldSet.add("e");
        Set<String> newSet = new HashSet<>();
        newSet.add("c");
        newSet.add("d");
        newSet.add("e");
        newSet.add("f");
        newSet.add("g");

        //剔除已经删除的元素
        CollectionUtil.subtract(oldSet, newSet).forEach(oldSet::remove);

        //添加新增的元素 下面两种写法等价
        //CollectionUtil.subtract(newSet, oldSet).forEach(oldSet::add);
        oldSet.addAll(CollectionUtil.subtract(newSet, oldSet));

        System.out.println(oldSet);
    }

    @Test
    public void test3() {
        ConcurrentHashMap<String, String> mp = new ConcurrentHashMap<>();
        mp.put("1", "A");
        mp.put("2", "B");
        mp.put("3", "C");

        System.out.println(mp);
        mp.forEach((k, v) -> mp.remove(k));
        System.out.println(mp);
    }

    @Test
    public void test4() {
        Set<String> set = new HashSet<>();
        set.add("a");
        set.add("b");
        set.add("c");
        set.add("d");
        set.add("e");

        set.forEach(item -> {
            if ("c".equals(item)) {
                return;
            }
            System.out.println(item);
        });
    }
}
