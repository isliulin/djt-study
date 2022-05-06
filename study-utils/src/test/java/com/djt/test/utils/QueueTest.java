package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.thread.ThreadUtil;
import org.junit.Test;

import java.util.*;

/**
 * 栈与队列测试
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-26
 */
public class QueueTest {

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

    @Test
    public void testPriorityQueue1() {
        Random random = new Random();
        PriorityQueue<Integer> queue = new PriorityQueue<>();
        for (int i = 0; i < 10; i++) {
            int v = random.nextInt(20);
            System.out.println("入队列:" + v);
            queue.offer(v);
            printQueue(queue, false);
        }
    }

    @Test
    public void testPriorityQueue2() {
        int lastMillis = 5000;
        PriorityQueue<Long> queue = new PriorityQueue<>();
        long tms = System.currentTimeMillis();
        long maxTms = tms;
        for (int i = 0; i < 10000; i++) {
            tms += 1000;
            if (tms > maxTms) {
                maxTms = tms;
            }
            System.out.println("入队列:" + LocalDateTimeUtil.of(tms).format(DatePattern.NORM_TIME_FORMATTER));
            queue.offer(tms);
            adjustQueue(queue, maxTms, lastMillis);
            printQueue(queue, true);
            ThreadUtil.sleep(1000);
        }
    }

    /**
     * 动态调整队列大小 只保留最近N秒的数据
     *
     * @param queue      队列
     * @param maxTms     队列最大时间
     * @param lastMillis 最近N毫秒
     */
    public void adjustQueue(Queue<Long> queue, long maxTms, int lastMillis) {
        if (queue == null || queue.isEmpty() || lastMillis < 0) {
            return;
        }
        while (true) {
            Long minTms = queue.peek();
            if (minTms != null && maxTms - minTms >= lastMillis) {
                queue.poll();
            } else {
                break;
            }
        }
    }

    /**
     * 打印队列
     *
     * @param queue       queue
     * @param isTimeStamp 是否时间戳
     * @param <E>         E
     */
    public <E> void printQueue(PriorityQueue<E> queue, boolean isTimeStamp) {
        if (queue == null || queue.isEmpty()) {
            return;
        }
        PriorityQueue<E> queueTmp = new PriorityQueue<>(queue);
        StringBuilder sb = new StringBuilder("[");
        while (!queueTmp.isEmpty()) {
            E v = queueTmp.poll();
            String vStr = v != null && isTimeStamp ?
                    LocalDateTimeUtil.of((Long) v).format(DatePattern.NORM_TIME_FORMATTER) : String.valueOf(v);
            sb.append(vStr);
            if (!queueTmp.isEmpty()) {
                sb.append(",");
            }
        }
        sb.append("]");
        System.out.println(sb);
    }

    @Test
    public void testBoundedPriorityQueue() {
        //BoundedPriorityQueue
    }

}
