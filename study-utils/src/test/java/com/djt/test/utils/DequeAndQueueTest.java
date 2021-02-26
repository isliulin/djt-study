package com.djt.test.utils;

import org.junit.Test;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

/**
 * 栈与队列测试
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-26 15:11
 */
public class DequeAndQueueTest {

    @Test
    public void testDeque() {
        //栈
        Deque<String> stack = new LinkedList<>();
        stack.push("A");
        stack.push("B");
        stack.push("C");

        System.out.println(stack.peek()); //获取栈顶元素 但不移除 空返回null
        System.out.println(stack.element()); //获取栈顶元素 但不移除 空抛异常
        System.out.println(stack.peek());

        System.out.println(stack.pop()); //获取栈顶元素 且移除
        System.out.println(stack.pop());
        System.out.println(stack.pop());
    }

    @Test
    public void testQueue() {
        //队列
        Queue<String> queue = new LinkedList<>();
        queue.offer("A");
        queue.offer("B");
        queue.offer("C");

        System.out.println(queue.peek()); //获取队头元素 但不移除 空返回null
        System.out.println(queue.element()); //获取队头元素 但不移除 空抛异常
        System.out.println(queue.peek());

        System.out.println(queue.poll()); //获取队头元素 且移除
        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
    }

}
