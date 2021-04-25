package com.djt.test.thread;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.*;

/**
 * 线程池测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-23
 */
@Slf4j
public class ThreadPoolTest {

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
            //try {
            //    future.get();
            //} catch (InterruptedException | ExecutionException e) {
            //    log.error("出错：{}", e.getMessage());
            //}
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            log.info("getTaskCount={} getActiveCount={} getCompletedTaskCount={}",
                    executor.getTaskCount(), executor.getActiveCount(), executor.getCompletedTaskCount());
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("所有线程执行完成...");
    }
}
