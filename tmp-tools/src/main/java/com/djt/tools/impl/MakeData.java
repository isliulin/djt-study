package com.djt.tools.impl;

import cn.hutool.core.date.DatePattern;
import com.djt.tools.AbsTools;
import com.djt.utils.KafkaUtils;
import com.djt.utils.MakeDataUtils;
import lombok.extern.log4j.Log4j2;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-06-25
 */
@Log4j2
public class MakeData extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        String topic = PROPS.getProperty("kafka.topic.event", null);
        long sleepMs = PROPS.getLong("kafka.send.sleep");
        LocalDateTime startTime = LocalDateTime.parse(PROPS.getStr("start.time", "1970-01-01 00:00:00"),
                DatePattern.NORM_DATETIME_FORMATTER);
        Properties properties = KafkaUtils.getProducerProps(PROPS.toProperties());
        MakeDataUtils.makeDataToKafka(topic, Integer.MAX_VALUE, sleepMs, startTime, properties);
    }
}
