package com.djt.test.utils;

import cn.hutool.core.math.MathUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 线程池测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-23
 */
@Slf4j
public class ThreadPoolTest {

    @Before
    public void before() {
        log.info("程序开始运行...");
    }

    @After
    public void after() {
        log.info("程序运行结束...");
    }

    @Test
    public void testThreadPool1() {
        log.info("本机CPU核数：{}", Runtime.getRuntime().availableProcessors());
        Random random = new Random();
        //ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        for (int i = 0; i < 10; i++) {
            Future<?> future = executor.submit(() -> {
                log.info("thread id is: {} name is: {}", Thread.currentThread().getId(), Thread.currentThread().getName());
                try {
                    Thread.sleep(random.nextInt(10000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //System.out.println(100 / 0);
            });
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("出错：{}", e.getMessage());
            }
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            log.info("getTaskCount={} getActiveCount={} getCompletedTaskCount={}",
                    executor.getTaskCount(), executor.getActiveCount(), executor.getCompletedTaskCount());
            ThreadUtil.sleep(1000);
        }
        log.info("所有线程执行完成...");
    }

    @Test
    public void testLock() {
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        AtomicBoolean done = new AtomicBoolean(false);
        //启动线程1 获取读锁
        executor.execute(() -> lockAndDo(readWriteLock, "READ", () -> {
            while (!done.get()) {
                ThreadUtil.sleep(10);
            }
        }));

        ThreadUtil.sleep(1000);

        //启动线程2 获取写锁
        executor.execute(() -> lockAndDo(readWriteLock, "WRITE", () -> {
            while (!done.get()) {
                ThreadUtil.sleep(10);
            }
        }));

        ThreadUtil.sleep(1000);

        //启动线程3 获取读锁
        executor.execute(() -> lockAndDo(readWriteLock, "READ", () -> {
            while (!done.get()) {
                ThreadUtil.sleep(10);
            }
        }));

        executor.shutdown();
        //10秒之后结束程序
        ThreadUtil.sleep(100000);
        done.set(true);
        ThreadUtil.sleep(1000);

    }

    /**
     * 加锁并执行任务
     *
     * @param readWriteLock 读写锁
     * @param lockType      读/写
     * @param task          待执行的任务
     */
    private void lockAndDo(ReadWriteLock readWriteLock, String lockType, Runnable task) {
        Lock lock;
        switch (StringUtils.trimToEmpty(lockType).toUpperCase()) {
            case "READ":
                lock = readWriteLock.readLock();
                break;
            case "WRITE":
                lock = readWriteLock.writeLock();
                break;
            default:
                throw new IllegalArgumentException(lockType + " 参数错误");
        }
        log.info("{} 尝试获取 {} 锁...", Thread.currentThread().getName(), lockType);
        lock.lock();
        try {
            log.info("{} 成功获取 {} 锁...", Thread.currentThread().getName(), lockType);
            task.run();
        } finally {
            lock.unlock();
            log.info("{} 已经释放 {} 锁...", Thread.currentThread().getName(), lockType);
        }
    }

    @Test
    public void testScheduler() {
        ScheduledExecutorService scheduler = ThreadUtil.createScheduledExecutor(1);
        scheduler.scheduleWithFixedDelay(() ->
                log.info("当前线程=>{}", Thread.currentThread().getName()), 10, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testThreadExecute() {
        Random random = new Random();
        ExecutorService pool = ThreadUtil.newExecutor(10, 10, 100);
        for (int i = 0; i < 10; i++) {
            int id = i;
            pool.execute(() -> {
                Thread.currentThread().setName("线程" + id);
                String name = Thread.currentThread().getName();
                System.out.println(StrUtil.format("{}开始执行", name));
                ThreadUtil.sleep(random.nextInt(10) * 1000);
                System.out.println(StrUtil.format("{}执行完成", name));
            });
        }
        pool.shutdown();
        try {
            if (!pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                throw new RuntimeException("超时等待");
            }
        } catch (InterruptedException | RuntimeException e) {
            e.printStackTrace();
        }
        System.out.println("所有线程执行完成");
    }

    @Test
    public void testThreadSubmit() {
        System.out.println(Math.ceil(5/2d));
        Random random = new Random();
        ExecutorService pool = ThreadUtil.newExecutor(10, 10, 100);
        List<Future<?>> futureList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int id = i;
            Future<?> future = pool.submit(() -> {
                Thread.currentThread().setName("线程" + id);
                String name = Thread.currentThread().getName();
                int randomInt = random.nextInt(10);
                System.out.println(StrUtil.format("{}开始执, 休眠{}s", name, randomInt));
                ThreadUtil.sleep(randomInt * 1000);
                if (randomInt % 2 == 0) {
                    throw new RuntimeException(name + " 发生异常");
                }
                System.out.println(StrUtil.format("{}执行完成", name));
            });
            futureList.add(future);
        }
        pool.shutdown();
        for (Future<?> future : futureList) {
            try {
                future.get(600, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        System.out.println("所有线程执行完成");
    }

}
