package com.djt.test.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.HashMultimap;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.junit.Test;

import java.util.*;
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

    @Test
    public void test5() {
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

    @Test
    public void test6() {
        HashMultimap<String, Integer> multimap = HashMultimap.create();
        multimap.put("A", 1);
        multimap.put("A", 2);
        multimap.put("A", 3);
        multimap.put("A", 3);
        multimap.put("B", 4);
        multimap.put("B", 5);
        multimap.put("B", 6);
        multimap.put("B", 6);
        System.out.println(multimap);
        multimap.get("A").remove(1);
        System.out.println(multimap);
    }

    @Test
    public void test7() {
        ListOrderedSet<Integer> list = new ListOrderedSet<>();
        list.add(7);
        list.add(5);
        list.add(8);
        list.add(3);
        list.add(9);
        list.add(2);
        list.add(0);
        list.add(4);
        list.add(6);
        list.add(1);
        System.out.println(list);
    }

    /**
     * 模拟大文件排序
     */
    @Test
    public void test8() {
        Random random = new Random();
        List<List<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            List<Integer> tmpList = new ArrayList<>();
            tmpList.add(random.nextInt(100));
            list.add(tmpList);
        }
        System.out.println(StrUtil.format("初始列表=>{}", list));

        while (list.size() > 1) {
            List<List<Integer>> tmpList = new ArrayList<>();
            Iterator<List<Integer>> iter = list.iterator();
            while (iter.hasNext()) {
                List<Integer> listA = iter.next();
                iter.remove();
                List<Integer> listMerged = new ArrayList<>(listA);
                if (iter.hasNext()) {
                    List<Integer> listB = iter.next();
                    listMerged.addAll(listB);
                    iter.remove();
                }
                Collections.sort(listMerged);
                tmpList.add(listMerged);
                System.out.println(StrUtil.format("当前排序=>{}", listMerged));
            }
            list.addAll(tmpList);
        }
        System.out.println(list);
    }

    @Test
    public void test9() {
        for (int i = 0; i < 10; i++) {
            try {
                testExp(i);
            } catch (Exception e) {
                System.err.println(i + ":" + e.getMessage());
                continue;
            }
            System.out.println(i);
        }
    }

    private void testExp(int i) {
        if (i % 2 == 0) {
            throw new IllegalArgumentException("异常");
        }
    }

    @Test
    public void testArrayUtil() {
        Integer[] arr = {1, 2, 3};
        System.out.println(ArrayUtil.containsAny(arr, 1));
        System.out.println(ArrayUtil.containsAny(arr, 4));
    }

    @Test
    public void testMapNull() {
        Map<String, Integer> map = new HashMap<>();
        map.put(null, 1);
        map.put("A", 2);
        System.out.println(map);
        int sum = map.values().stream().mapToInt(x -> x).sum();
        System.out.println(sum);
    }

}
