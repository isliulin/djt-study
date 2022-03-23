package com.djt.tools.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.djt.tools.AbsTools;
import com.djt.utils.KafkaUtils;
import com.djt.utils.MakeDataUtils;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-06-25
 */
public class MakeData extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        String topic = PROPS.getProperty("kafka.topic.event", null);
        long sleepMs = PROPS.getLong("kafka.send.sleep");
        LocalDateTime startTime = LocalDateTime.parse(PROPS.getStr("start.time",
                LocalDateTimeUtil.beginOfDay(LocalDateTime.now()).format(DatePattern.NORM_DATETIME_FORMATTER)),
                DatePattern.NORM_DATETIME_FORMATTER);

        int interval = PROPS.getInt("event.time.interval", 10);
        Properties properties = KafkaUtils.getProducerProps(PROPS.toProperties());
        MakeDataUtils.makeDataToKafka(topic, Integer.MAX_VALUE, sleepMs, startTime, interval, properties);
    }
}
