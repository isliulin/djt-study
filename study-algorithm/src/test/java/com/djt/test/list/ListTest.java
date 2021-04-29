package com.djt.test.list;

import com.djt.algorithm.list.LinkedList;
import com.djt.utils.LinkedListUtils;
import com.djt.utils.RandomUtils;
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
            int num = RandomUtils.getRandomNumber(0, 10);
            list.insert(num);
        }
        LinkedListUtils.printList(list);
    }

    @Test
    public void test1() {

    }

}
