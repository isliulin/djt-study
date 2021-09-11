package com.djt.event;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * flink将kafka消息自动转JSONObject
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-21
 */
@Log4j2
public class JsonSchema implements DeserializationSchema<JSONObject>, SerializationSchema<JSONObject> {

    @Override
    public JSONObject deserialize(byte[] message) {
        String str = new String(message, StandardCharsets.UTF_8);
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(str);
        } catch (Exception e) {
            log.error("此消息转换Json错误: {}\n", str, e);
        }
        return jsonObject;
    }

    @Override
    public byte[] serialize(JSONObject element) {
        return element.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
