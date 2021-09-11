package com.djt.event;

import com.alibaba.fastjson.JSON;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * 自定义消息Schema
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-21
 */
@Log4j2
public class MySchema implements DeserializationSchema<MyEvent>, SerializationSchema<MyEvent> {

    @Override
    public MyEvent deserialize(byte[] message) {
        return JSON.parseObject(message, MyEvent.class);
    }

    @Override
    public byte[] serialize(MyEvent element) {
        return JSON.toJSONBytes(element);
    }

    @Override
    public boolean isEndOfStream(MyEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MyEvent> getProducedType() {
        return TypeInformation.of(MyEvent.class);
    }
}
