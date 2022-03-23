package com.djt.tools.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.djt.tools.AbsTools;
import com.djt.utils.MakeDataUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class FileToKafka extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        String filePath = PROPS.getStr("kafka.producer.file.path");
        String topic = PROPS.getProperty("kafka.topic.event", null);
        long sleepMs = PROPS.getLong("kafka.send.sleep");
        boolean producerLog = PROPS.getBool("kafka.producer.log.enable", false);
        LocalDateTime startTime = LocalDateTime.parse(PROPS.getStr("start.time",
                LocalDateTimeUtil.beginOfDay(LocalDateTime.now()).format(DatePattern.NORM_DATETIME_FORMATTER)),
                DatePattern.NORM_DATETIME_FORMATTER);
        LocalDate startDate = startTime.toLocalDate();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            startDate = startDate.plusDays(i);
            MakeDataUtils.readFileToKafka(filePath, startDate, sleepMs, topic, PROPS, producerLog);
        }
    }

}
