package com.djt.entity;

import cn.hutool.core.date.DatePattern;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 监控信息
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-07
 */
@Data
public class MetricEntity {

    @JSONField(name = "group")
    private String group;

    @JSONField(name = "key")
    private String key;

    @JSONField(name = "value")
    private double value;

    @JSONField(name = "c_time")
    private String cTime = LocalDateTime.now().format(DatePattern.NORM_DATETIME_FORMATTER);

    @JSONField(name = "batch_no")
    private long batchNo;

    @JSONField(name = "id")
    public void setKey(String key) {
        this.key = key;
    }
}
