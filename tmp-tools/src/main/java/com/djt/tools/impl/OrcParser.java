package com.djt.tools.impl;

import cn.hutool.core.date.DatePattern;
import com.djt.tools.AbsTools;
import com.djt.utils.KafkaUtils;
import com.djt.utils.MakeDataUtils;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class OrcParser extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        String file = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\orc-001";
        String[] dateArr = {"2022-01-01 00:00:00", "2022-01-02 00:00:00", "2022-01-03 00:00:00"};
        String topic = PROPS.getProperty("kafka.topic.event", null);
        long sleepMs = PROPS.getLong("kafka.send.sleep");
        Properties properties = KafkaUtils.getProducerProps(PROPS.toProperties());
        for (String date : dateArr) {
            LocalDateTime startTime = LocalDateTime.parse(date, DatePattern.NORM_DATETIME_FORMATTER);
            MakeDataUtils.readOrcToKafka(file, startTime, 21L, topic, sleepMs, properties);
        }
    }

}
