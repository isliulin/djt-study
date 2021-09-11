package com.djt.event;

import com.alibaba.fastjson.JSON;
import com.djt.utils.EventUtils;
import org.apache.flink.table.api.ApiExpression;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-08-30
 */
public class EventTest {

    @Test
    public void testJsonSer() {
        String json = "{\n" +
                "  \"eventTime\": 1630125296000,\n" +
                "  \"id\": \"666\",\n" +
                "  \"name\": \"张三\",\n" +
                "  \"num\": 100,\n" +
                "  \"time\": \"2021-08-28 12:34:56\"\n" +
                "}";

        MyEvent event = JSON.parseObject(json, MyEvent.class);
        System.out.println(event);

        json = JSON.toJSONString(event);
        System.out.println(json);
    }

    @Test
    public void testGetExpressions() {
        ApiExpression[] expressions = EventUtils.getExpressions(MyEvent.class);
        for (ApiExpression expression : expressions) {
            System.out.println(expression.asSummaryString());
        }
    }

}
