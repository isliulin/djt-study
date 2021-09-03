package com.djt.event;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.djt.enums.EventType;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;

/**
 * 事件记录
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-22
 */
@Log4j2
@Data
public class EventRecord implements Serializable {

    /**
     * 事件ID
     */
    @JSONField(name = "event_id", ordinal = 1)
    private String eventId;

    /**
     * 事件类型
     */
    @JSONField(name = "type", ordinal = 2)
    private String eventType;

    /**
     * 时间戳
     */
    @JSONField(name = "timestamp", ordinal = 3)
    private Long timestamp;

    /**
     * 事件值
     */
    @JSONField(name = "event", ordinal = 4)
    private JSONObject event;

    /**
     * 将json转换为具体的事件对象
     *
     * @param event json
     */
    public void setEvent(JSONObject event) {
        this.event = event;
        this.eventValue = EventType.parseEventValue(eventType, event);
        if (this.eventValue != null) {
            this.eventValue.setEventRecord(this);
        } else {
            log.warn("未知事件类型：{}", eventType);
        }
    }

    /**
     * 事件对象 由event转换得来
     * 该字段不进行序列化和反序列化
     */
    @JSONField(serialize = false, deserialize = false)
    private BaseEvent eventValue;

}
