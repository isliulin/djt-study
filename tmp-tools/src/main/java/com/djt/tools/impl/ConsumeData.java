package com.djt.tools.impl;

import com.djt.tools.AbsTools;
import com.djt.utils.KafkaUtils;
import lombok.extern.log4j.Log4j2;

import java.util.Properties;

/**
 * 消费kafka数据
 *
 * @author 　djt317@qq.com
 * @since 　 2021-06-25
 */
@Log4j2
public class ConsumeData extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        String topic = PROPS.getProperty("kafka.topic.event", null);
        Properties properties = KafkaUtils.getConsumerProps(PROPS.toProperties());
        KafkaUtils.startConsumer(topic, 1000, false, properties);
    }
}
