package com.djt.test.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-10-25
 */
public class KafkaUtilsTest {

    @Test
    public void test1() {
        for (String key : ConsumerConfig.configNames()) {
            System.out.println(key);
        }
    }

}
