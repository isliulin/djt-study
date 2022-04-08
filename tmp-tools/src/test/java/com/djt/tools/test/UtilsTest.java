package com.djt.tools.test;

import com.djt.utils.MakeDataUtils;
import org.junit.Test;

import java.time.LocalDate;

import static com.djt.tools.AbsTools.PROPS;

/**
 * 工具类测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2022-03-11
 */
public class UtilsTest {

    @Test
    public void testReadFileToKafka() {
        String file = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303";
        MakeDataUtils.readFileToKafka(file, LocalDate.now(), 1000, "RISK_ANALYSIS_EVENT_TMP", PROPS, true);
    }

    @Test
    public void testProp() {
        System.out.println(PROPS.get("test.line"));
    }

}

