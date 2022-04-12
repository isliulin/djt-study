package com.djt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.djt.utils.DjtConstant;
import lombok.Data;

import java.time.ZonedDateTime;

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
    private String cTime = ZonedDateTime.now().format(DjtConstant.YMDHMSZ_FORMAT);

    @JSONField(name = "batch_no")
    private long batchNo;

    @JSONField(name = "id")
    public void setKey(String key) {
        this.key = key;
    }
}
