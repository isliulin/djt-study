package com.djt.event;

import com.alibaba.fastjson.JSON;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * flink将kafka消息自动转Event
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-21
 */
@Log4j2
public class EventSchema implements DeserializationSchema<EventRecord>, SerializationSchema<EventRecord> {

    @Override
    public EventRecord deserialize(byte[] message) {
        EventRecord eventRecord = null;
        try {
            eventRecord = JSON.parseObject(message, EventRecord.class);
        } catch (Exception e) {
            log.error("此消息格式转换错误: {}\n", new String(message, StandardCharsets.UTF_8), e);
        }
        return eventRecord;
    }

    @Override
    public byte[] serialize(EventRecord element) {
        return JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(EventRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<EventRecord> getProducedType() {
        return TypeInformation.of(EventRecord.class);
    }
}
