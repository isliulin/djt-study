package com.djt.test.list;

import com.djt.algorithm.list.LinkedList;
import com.djt.utils.LinkedListUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * 链表测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-29
 */
public class ListTest {

    LinkedList<Integer> list = new LinkedList<>();

    @Before
    public void before() {
        for (int i = 0; i < 10; i++) {
            list.insert(i);
        }
        LinkedListUtils.printList(list);
    }

    @Test
    public void testReverse1() {
        list.reverse();
        LinkedListUtils.printList(list);
    }

    @Test
    public void testReverse2() {
        list.reverse2();
        LinkedListUtils.printList(list);
    }

}
